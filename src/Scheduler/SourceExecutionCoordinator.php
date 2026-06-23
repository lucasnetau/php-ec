<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Scheduler;

use Clue\React\Zlib\Decompressor;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Library\Histogram;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\React\Decoder as JsonRpcDecoder;
use Evenement\EventEmitterInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LogLevel;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use Throwable;
use function array_merge;
use function count;
use function gettype;
use function in_array;
use function is_array;
use function is_string;
use function is_subclass_of;
use function json_encode;
use function strlen;
use function trim;

class SourceExecutionCoordinator implements EventEmitterInterface, LoggerAwareInterface
{
    use \Evenement\EventEmitterTrait;

    use LoggerAwareTrait;

    /**
     * @var Process[]|SourceFunction[] Process table for running input processes
     */
    protected array $input_processes = [];

    /**
     * @var array Process Configuration for input processes
     */
    protected array $input_processes_config = [];

    /**
     * @var array
     */
    protected array $input_processes_checkpoints = [];

    /**
     * @var array<string, Histogram>
     */
    protected array $inputRpcPacketSizes = [];

    /**
     * @var LoopInterface
     */
    protected LoopInterface $loop;

    /**
     * @var bool Flag to track if initialise_input_processes has been called
     */
    protected bool $initialized = false;

    /**
     * @var bool Flag indicating if the scheduler is shutting down
     */
    protected bool $stopping = false;

    public function __construct(LoopInterface $loop)
    {
        $this->loop = $loop;
    }

    /**
     * @param string|int $id Descriptive and unique key of the input process
     * @param string|array|SourceFunction $cmd Command to execute
     * @param string|null $wd Working directory for the process
     * @param array $env Key-value pair describing Environmental variables to pass to the process
     * @param bool $essential If true, Scheduler will shut everything down if this input process exit with no errorCode or doesn't exist
     * @param bool $autostart Whether to start the process automatically on startup or when requested
     */
    public function register_input_process(string|int $id, string|array|SourceFunction $cmd, ?string $wd = null, array $env = [], bool $essential = false, bool $autostart = true) : void
    {
        $this->input_processes_config[$id] = [
            'cmd' => $cmd,
            'wd' => $wd,
            'env' => $env,
            'essential' => $essential,
            'autostart' => $autostart
        ];
    }

    /**
     * @param string|int $id
     * @return void
     */
    public function unregister_input_process(string|int $id) : void
    {
        unset($this->input_processes_config[$id]);
    }

    /**
     * @return void
     * @throws Throwable
     */
    public function initialise_input_processes() : void {
        if ($this->initialized) {
            return;
        }

        $this->initialized = true;

        $this->logger->debug('Initialising input processes');
        foreach ($this->input_processes_config as $id => $config) {
            if ($config['autostart'] === true) {
                $this->initialise_input_process($id);
            }
        }
    }

    /**
     * @param int|string $id
     * @return Process|SourceFunction
     */
    public function initialise_input_process(int|string $id) : Process|SourceFunction {
        $config = $this->input_processes_config[$id];
        if (is_subclass_of($config['cmd'], SourceFunction::class)) {
            return $this->setup_source_function($id);
        } else {
            return $this->start_input_process($id);
        }
    }

    protected function setup_source_function(int|string $id): SourceFunction
    {
        $config = $this->input_processes_config[$id];
        /** @var SourceFunction $cmd */
        $cmd = is_string($config['cmd']) ? new $config['cmd']() : clone $config['cmd'];

        $this->logger->debug("Initialising source function $id");
        $cmd->setLogger($this->logger);
        $this->input_processes[$id] = $cmd;

        $this->inputRpcPacketSizes[$id] = new Histogram(64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576);

        $cmd->on('data', function(Event $event) use ($id) {
            $this->inputRpcPacketSizes[$id]->add(strlen(json_encode($event)));
            $this->emit('event', [$event]);
        });
        $cmd->on('checkpoint', function($checkpoint) use($id) {
            $this->input_processes_checkpoints[$id] = $checkpoint;
            $this->emit('dirty');
        });
        $cmd->on('error', function(Throwable $error) use ($id) {
            $this->emit('error', ['id' => $id, 'exception' => $error]);
        });
        $cmd->on('exit', function($code) use($id) {
            /** Remove from process table */
            unset($this->input_processes[$id]);
            $level = ($code === 0) ? LogLevel::INFO : LogLevel::ERROR;
            $this->logger->log($level,"Input Process {id} exited with code: {code}", ['id' => $id, 'code' => $code,]);

            /** Restart the input process if it exits with an error code */
            if (!$this->isStopping() && $code === 0 && $this->input_processes_config[$id]['essential']) {
                $this->emit('essential_exit', [$id]);
                return;
            }
            /** We stop processing if there are no input processes available **/
            if (0 === count($this->input_processes)) {
                $this->emit('all_processes_stopped');
            }
        });
        $checkpoint = $this->input_processes_checkpoints[$id] ?? null;
        $this->loop->futureTick(static function() use ($cmd, $config, $checkpoint) {
            $cmd->start($config['env'], $checkpoint);
        });
        return $cmd;
    }

    /**
     * @param int|string $id
     * @return Process
     */
    protected function start_input_process(int|string $id) : Process {
        if (isset($this->input_processes[$id])) {
            $this->logger->critical('Input process ' . $id . ' already running');
            return $this->input_processes[$id];
        }
        $config = $this->input_processes_config[$id];
        $env = $config['env'];
        /** If we have a checkpoint in our save state pass this along to the input process via the ENV */
        if (isset($this->input_processes_checkpoints[$id]))
        {
            $env = array_merge([Scheduler::CHECKPOINT_VARNAME => json_encode($this->input_processes_checkpoints[$id])], $env);
        }
        if (is_string($config['cmd'])) {
            /** Use exec to ensure process receives our signals and not the bash wrapper */
            $process = new Process('exec ' . $config['cmd'], $config['wd'], $env);
        } else {
            /** When passed an array, PHP will open the command directly without going through a shell */
            $process = new Process($config['cmd'], $config['wd'], $env);
        }
        $this->setup_input_process($process, $id);
        if ($process->isRunning()) {
            $this->logger->info("Started input process $id");
            $this->input_processes[$id] = $process;
            if (\function_exists('\posix_setpgid')) {
                \posix_setpgid($process->getPid(), 0);
            }
            return $process;
        }
        throw new \RuntimeException('Input process ' . $id . ' failed to start');
    }

    /**
     * @param Process $process
     * @param int|string $id
     */
    protected function setup_input_process(Process $process, int|string $id): void
    {
        try {
            $process->start($this->loop);
        } catch (\RuntimeException $exception) {
            $this->logger->critical("Failed to start input process {id}", ['id' => $id, 'exception' => $exception,]);
            return;
        }

        // raw DEFLATE decompression for source stdout. PHP's zlib.deflate stream filter produces raw DEFLATE (RFC 1951), not GZIP.
        $compress = ($this->input_processes_config[$id]['env']['PHPEC_RPC_COMPRESSION'] ?? '') === '1';
        if ($compress) {
            $decompressor = new Decompressor(ZLIB_ENCODING_RAW);
            $process->stdout->pipe($decompressor);
            $process->stdout = $decompressor;
        }

        $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

        $this->inputRpcPacketSizes[$id] = new Histogram(64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576);

        /**
         * Handle RPC call from the input process
         */
        $process_decoded_stdout->on('data', function(JsonRpcNotification $rpc) use ($id) {
            $json = json_encode($rpc);
            $this->inputRpcPacketSizes[$id]->add(strlen($json));
            switch ( $rpc->getMethod() ) {
                case Scheduler::INPUT_ACTION_HANDLE:
                    $eventData = $rpc->getParam('event');
                    if (!is_array($eventData)) {
                        $this->logger->critical("Input process did not give us an event array to handle. Received type: {type}, value: {value} ", ['type' => gettype($eventData), 'value' => json_encode($eventData),]);
                        return;
                    }
                    $event = new Event($eventData);
                    $this->emit('event', [$event]);
                    break;
                case Scheduler::INPUT_ACTION_CHECKPOINT:
                    $this->input_processes_checkpoints[$id] = $rpc->getParams();
                    $this->emit('dirty');
                    break;
                case Scheduler::RPC_PROCESS_LOG:
                    $this->emit('log', ['logLevel' => $rpc->getParam('logLevel'), 'message' => $rpc->getParam('message'), 'context' => $rpc->getParam('context') ?? []]);
                    break;
                default:
                    throw new \RuntimeException("Unknown json rpc command {$rpc->getMethod()} from input process");
            }
        });

        /**
         * Log any errors we receive on the processed STDERR, error is a fatal event, and the stream will be closed, so we need to terminate the process since it can no longer communicate with us
         */
        $process_decoded_stdout->on('error', function(Throwable $error) use ($id, $process) {
            $this->emit('error', ['id'=> $id, 'exception' => $error,]);
            $process->terminate(SIGTERM);
        });

        /**
         * Input processes STDOUT has closed, if we are not in the process of shutting down then we need to terminate the process since it can no longer communicate with us
         */
        $process->stdout->on('close', function() use ($id, $process) {
            if (!$this->isStopping() && $process->isRunning()) {
                //Use a timer to get around a race condition in Alpine linux where the process hasn't been reaped yet (SIGCHLD delayed?)
                $this->loop->addTimer(0.1, function() use ($id, $process) {
                    if ($process->isRunning()) {
                        $this->logger->critical("{id} STDOUT closed unexpectedly, terminating process", ['id' => $id,]);
                        $process->terminate(SIGTERM);
                    }
                });
            }
        });

        /**
         * Log STDERR messages from input processes
         */
        $process->stderr->on('data', function($data) use ($id) {
            $this->logger->error("$id message: " . trim($data));
        });

        /**
         * Handle process exiting
         */
        $process->on('exit', function($code, $term) use($id) {
            /** Remove process from table  */
            unset($this->input_processes[$id]);

            if ($term === null) {
                $level = ($code === 0) ? LogLevel::INFO : LogLevel::ERROR;
                $this->logger->log($level,"Input Process {id} exited with code: {code}", ['id' => $id, 'code' => $code,]);
            } else {
                $this->logger->info("Input Process $id exited on signal: $term");
            }
            if ($code === 127) {
                $this->logger->critical("Input process $id exit was due to Command Not Found");
            } elseif ($code === 255) { //255 = PHP Fatal exit code
                $this->logger->critical("Input process $id exit was due to fatal PHP error");
            }
            /** Restart the input process if it exits with an error code EXCEPT if it is exit code 127 - Command Not Found */
            if (!$this->isStopping()) {
                if (!in_array($code, [0, 127], true)) {
                    $this->logger->debug("Restarting process $id");
                    $this->start_input_process($id);
                } elseif ($this->input_processes_config[$id]['essential']) {
                    if ($code === 127) {
                        $this->logger->info("Essential input process cannot be found. Shutting down");
                    } else {
                        $this->logger->info("Essential input process has stopped cleanly. Shutting down");
                    }
                    $this->emit('essential_exit', [$id]);
                    return;
                }
            }
            /** We stop processing if there are no input processes available **/
            if (0 === count($this->input_processes)) {
                $this->emit('all_processes_stopped');
            }
        });
    }

    /**
     * Pause all running processes (SIGSTOP)
     */
    public function pauseAllProcesses(): void
    {
        foreach ($this->input_processes as $processId => $process) {
            if ($process->isRunning()) {
                $process->terminate(SIGSTOP);
                $this->logger->debug("Paused input process {id}", ['id' => $processId,]);
            }
        }
    }

    /**
     * Resume all paused processes (SIGCONT)
     */
    public function resumeAllProcesses(): void
    {
        foreach ($this->input_processes as $processId => $process) {
            $process->terminate(SIGCONT);
            $this->logger->debug("Resuming input process {id}", ['id' => $processId,]);
        }
    }

    /**
     * Shutdown all processes
     */
    public function shutdown(): void
    {
        $this->logger->debug("Shutting down running input processes");
        foreach ($this->input_processes as $processKey => $process) {
            $this->logger->debug("Sending SIGTERM to input process $processKey");
            if (false === $process->terminate(SIGTERM)) {
                $this->logger->error("Unable to send SIGTERM to input process $processKey");
            }
            if ($process->isStopped()) {
                $process->terminate(SIGCONT); // If our input processes are paused by memory pressure then we need to send SIGCONT after SIGTERM as they are currently stopped
            }
        }
    }

    /**
     * @return array
     */
    public function getInputProcesses(): array
    {
        return $this->input_processes;
    }

    /**
     * @return array
     */
    public function getInputProcessesCheckpoints(): array
    {
        return $this->input_processes_checkpoints;
    }

    /**
     * @param array $checkpoints
     */
    public function setInputProcessesCheckpoints(array $checkpoints): void
    {
        $this->input_processes_checkpoints = $checkpoints;
    }

    /**
     * @return array<string, Histogram>
     */
    public function getInputRpcPacketSizes(): array
    {
        return $this->inputRpcPacketSizes;
    }

    /**
     * @return array
     */
    public function getInputProcessesConfig(): array
    {
        return $this->input_processes_config;
    }

    /**
     * @param string|int $id
     * @return bool
     */
    public function hasProcess(string|int $id): bool
    {
        return isset($this->input_processes[$id]);
    }

    /**
     * @return int
     */
    public function getRunningProcessCount(): int
    {
        return count($this->input_processes);
    }

    /**
     * @return bool
     */
    public function isStopping(): bool
    {
        return $this->stopping;
    }

    /**
     * @param bool $stopping
     */
    public function setStopping(bool $stopping): void
    {
        $this->stopping = $stopping;
    }
}
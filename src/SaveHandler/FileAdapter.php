<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\SaveHandler;

use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\React\Decoder as JsonRpcDecoder;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use Evenement\EventEmitterTrait;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use RuntimeException;
use function bin2hex;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function dirname;
use function error_get_last;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function hrtime;
use function json_decode;
use function json_encode;
use function json_last_error;
use function json_last_error_msg;
use function random_bytes;
use function realpath;
use function rename;
use function round;
use function strlen;
use function tempnam;
use function trim;
use function unlink;

class FileAdapter implements SaveHandlerInterface {
    use EventEmitterTrait;

    /**
     * @var bool Flag to ensure we only have one save going on at a time
     */
    protected bool $asyncSaveInProgress = false;

    /**
     * @var int Size of the save state file
     */
    protected int $saveStateSizeBytes = 0;

    /** @var int Time in millisecond for last save handler to complete */
    protected int $saveStateLastDuration = 0;

    /** @var Process|null Handle to the external process to write out state in an async manner
     */
    protected ?Process $process = null;

    protected int $asyncFailureCount = 0;

    protected bool $asyncFaulty = false;

    public function __construct( protected string $saveFileName, protected LoggerInterface $logger, protected ?LoopInterface $loop ) {
        $this->loop ??= Loop::get();
    }

    public function saveStateAsync(array $state): void
    {
        if ($this->asyncFaulty) {
            $this->saveStateSync($state);
            return;
        }

        if ($this->asyncFailureCount === 10) {
            $this->logger->emergency('Async save handler exiting to often, moving to sync save');
            $this->asyncFaulty = true;
            $this->saveStateSync($state);
            return;
        }

        $this->asyncSaveInProgress = true;

        if ($this->process === null) {
            $this->process = new Process(php_cmd(realpath(__DIR__ . '/../../bin/save_state.php'), true),
                dirname($this->saveFileName), ['SAVESTATE_FILENAME' => $this->saveFileName]);
            $this->process->start($this->loop);

            $this->process->on('exit', function () {
                if ($this->asyncSaveInProgress) {
                    $this->asyncFailureCount++;
                    $this->logger->critical('Save state process exited during save');
                    $this->emit('save:failed', ['exception' => new RuntimeException('Save state process exited during save')]);
                }
                $this->asyncSaveInProgress = false;
                $this->process = null;
            });

            /**
             * Log STDERR messages from save process
             * @psalm-suppress PossiblyNullReference
             */
            $this->process->stderr->on('data', function ($data) {
                $this->logger->error("save handler message: " . trim($data));
            });

            /** @psalm-suppress PossiblyInvalidArgument */
            $process_decoded_stdout = new JsonRpcDecoder($this->process->stdout);

            $process_decoded_stdout->on('data', function ($rpc) {
                if ($rpc instanceof JsonRpcResponse) {
                    $this->asyncSaveInProgress = false;
                    if ($rpc->isSuccess()) {
                        $this->asyncFailureCount = 0;
                        $result = $rpc->getResult();
                        $this->saveStateSizeBytes = $result['saveStateSizeBytes'];
                        $this->saveStateLastDuration = (int)round((hrtime(true) - $result['saveStateBeginTime'])/1e+6); //Milliseconds
                        if ($this->saveStateLastDuration > 5000) {
                            $this->logger->warning('It took ' . $this->saveStateLastDuration . ' milliseconds to save ' . round($this->saveStateSizeBytes/1048576, 2) . 'MB state to disk');
                        }
                    } else {
                        $error = $rpc->getError();
                        /**
                         * @psalm-suppress PossiblyNullReference
                         */
                        $this->emit('save:failed', ['exception' => new RuntimeException($error->getMessage() . " : " . json_encode($error->getData()))]);
                    }
                } elseif ($rpc instanceof JsonRpcNotification) {
                    if ($rpc->getMethod() === Scheduler::RPC_PROCESS_LOG) {
                        //Log action expects logLevel to match \Psr\Log\LogLevel
                        $this->logger->log($rpc->getParam('logLevel'), $rpc->getParam('message'));
                    }
                }
            });

            $this->logger->debug('Initialised save handler process');
        }
        $state = json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
        if ($state === false) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Encoding application state failed. " . json_last_error_msg())] );
            return;
        }

        $uniqid = round(hrtime(true)/1e+3) . '.' . bin2hex(random_bytes(4));
        $rpc_request = new JsonRpcRequest(Scheduler::ACTION_RUN_METHOD, ['state' => $state, 'time' => hrtime(true)], $uniqid);
        /**
         * ReactPHP Process typehints aren't helpful here. stdin will be defined and be of type WritableStreamInterface
         * @psalm-suppress PossiblyUndefinedMethod
         * @psalm-suppress PossiblyNullReference
         */
        $this->process->stdin->write(json_encode($rpc_request) . "\n");
    }

    public function saveStateSync(array $state): void
    {
        $directory = dirname($this->saveFileName);
        $filename = tempnam($directory, ".php-ce.state.tmp");
        if (false === $filename) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Error creating temporary save state file, check filesystem")] );
            return;
        }
        $state = json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION | JSON_PARTIAL_OUTPUT_ON_ERROR);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->warning('Error encoding application state');
        }
        if ($state === false) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Encoding application state failed. " . json_last_error_msg())] );
            return;
        }
        $saveStateSize = strlen($state);
        $saveStateBegin = hrtime(true);
        if (!(@file_put_contents($filename, $state) === $saveStateSize && rename($filename, $this->saveFileName))) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Save state sync failed." . json_encode(error_get_last()))] );
            if (file_exists($filename) && !unlink($filename)) {
                $this->logger->warning('Unable to delete temporary save file');
            }
            return;
        }
        $this->saveStateLastDuration = (int)round((hrtime(true) - $saveStateBegin)/1e+6); //Milliseconds
        $this->saveStateSizeBytes = $saveStateSize;

        if ($this->saveStateLastDuration > 5000) {
            $this->logger->warning('It took ' . $this->saveStateLastDuration . ' milliseconds to save state to disk');
        }
        $this->logger->debug('State saved to filesystem');
        $this->asyncFailureCount = 0; //Reset the async failure count to re-enable async save after a successful sync save
    }

    public function asyncSaveInProgress(): bool
    {
        return $this->asyncSaveInProgress;
    }

    public function lastSaveSizeBytes() : int {
        return $this->saveStateSizeBytes;
    }

    public function lastSaveWriteDuration() : int {
        return $this->saveStateLastDuration;
    }

    public function loadState() : false|array {
        if (file_exists($this->saveFileName))
        {
            $contents = file_get_contents($this->saveFileName);
            if ($contents === false || $contents === '') {
                throw new RuntimeException('State file exists but contents could not be read or were empty. Exiting');
            }
            $state = json_decode($contents, true);
            if ($state === null) {
                throw new RuntimeException('Save state file was corrupted, Exiting. JSON Error: ' . json_last_error_msg() );
            }
            return $state;
        }
        return false;

    }

    public function __destruct()
    {
        $this->process?->terminate(SIGTERM);
    }
}
<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation;

use DateTimeImmutable;
use Exception;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\ChildProcess\Process;
use React\EventLoop\TimerInterface;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\React\Decoder as JsonRpcDecoder;
use React\Filesystem\Filesystem;
use React\Filesystem\FilesystemInterface;
use RuntimeException;

use function array_key_first;
use function error_get_last;
use function gettype;
use function is_array;
use function tempnam;
use function json_encode;
use function json_decode;
use function memory_get_usage;
use function count;
use function file_exists;
use function file_put_contents;
use function file_get_contents;
use function rename;
use function get_class;
use function gc_collect_cycles;
use function gc_mem_caches;
use function array_merge;
use function mt_rand;
use function ini_get;
use function preg_match;
use function strtoupper;
use function trim;
use function unlink;

use const SIGINT;
use const SIGTERM;
use const SIGHUP;

/**
 * Class Scheduler
 * @package EdgeTelemetrics\EventCorrelation
 * @property LoggerInterface $logger
 */
class Scheduler implements LoggerAwareInterface {
    /** PSR3 logger provides $this->logger */
    use LoggerAwareTrait;

    const CHECKPOINT_VARNAME = 'PHPEC_CHECKPOINT';

    /**
     * @var LoopInterface
     */
    protected LoopInterface $loop;

    /**
     * @var CorrelationEngine
     */
    protected CorrelationEngine $engine;

    /**
     * @var ?TimerInterface
     */
    protected ?TimerInterface $nextTimer = null;

    /**
     * @var DateTimeImmutable
     */
    protected DateTimeImmutable $timerScheduledAt;

    /**
     * @var Process[] Process table for running input processes
     */
    protected array $input_processes = [];

    /**
     * @var string[] Class list of rules
     */
    protected array $rules = [];

    /**
     * @var array Process Configuration for input processes
     */
    protected array $input_processes_config = [];

    /**
     * @var array
     */
    protected array $input_processes_checkpoints = [];

    /**
     * @var Process[] Process table to keep track of all action processes that are running.
     */
    protected array $runningActions = [];

    /**
     * @var array Configuration for actions
     */
    protected array $actionConfig = [];

    /**
     * @var array
     */
    protected array $inflightActionCommands = [];

    /**
     * @var array
     */
    protected array $erroredActionCommands = [];

    /**
     * @var TimerInterface Reference to the periodic task to save state.
     */
    protected TimerInterface $saveHandler;

    /**
     * @var string Filename to save state to
     */
    protected string $saveFileName = '/tmp/php-ec-savepoint';

    /**
     * @var bool Flag if the scheduler has information that needs to be flushed to the save file.
     */
    protected bool $dirty;

    /**
     * @var bool Flag to ensure we only have one save going on at a time
     */
    protected bool $asyncSaveInProgress = false;

    /**
     * @var float How ofter to save state
     */
    protected float $saveStateSeconds = 1;

    /**
     * Flag for whether we keep failed actions when loading or drop them.
     */
    const PRESERVE_FAILED_EVENTS_ONLOAD = false;

    /**
     * Event type when we start the engine up with no previous state.
     */
    const CONTROL_MSG_NEW_STATE = 'PHP-EC:Engine:Start';

    /**
     * Event type when we start the engine up restored state.
     */
    const CONTROL_MSG_RESTORED_STATE = 'PHP-EC:Engine:Restored';

    /**
     * Event type when we stop the engine.
     */
    const CONTROL_MSG_STOP = 'PHP-EC:Engine:Stop';

    /** @var int Level at which the memory pressure is considered resolved */
    const MEMORY_PRESSURE_LOW_WATERMARK = 35;

    /** @var int Level at which memory pressure mitigation is undertaken */
    const MEMORY_PRESSURE_HIGH_WATERMARK = 50;

    /** @var int Max outstanding actions before mitigation action is taken  */
    const RUNNING_ACTION_LIMIT_HIGH_WATERMARK = 30000;

    /** @var int Level at which outstanding action mitigation action is resolved */
    const RUNNING_ACTION_LIMIT_LOW_WATERMARK = 500;

    /** @var string|null */
    protected ?string $newEventAction = null;

    /** @var string RPC Method expected to handle an event */
    const INPUT_ACTION_HANDLE = 'handle';

    /** @var string RPC Method to record a checkpoint for an import process */
    const INPUT_ACTION_CHECKPOINT = 'checkpoint';

    /** @var string RPC Method to log a message from an import process */
    const RPC_PROCESS_LOG = 'log';

    /** @var array Accepted input RPC methods */
    const INPUT_ACTIONS = [ self::INPUT_ACTION_HANDLE, self::INPUT_ACTION_CHECKPOINT, self::RPC_PROCESS_LOG ];

    /** @var string RPC method name to get an action handler to run a request */
    const ACTION_RUN_METHOD = 'run';

    /** @var int */
    protected int $memoryLimit = 0;

    /** @var int */
    protected int $currentMemoryPercentUsed = 0;

    /** @var bool Flag tracking if we have paused dispatching */
    protected bool $pausedOnMemoryPressure = false;

    /** @var TimerInterface|null Timer used to ensure that we don't get stuck doing nothing if all actions complete but memory pressure stays above the high watermark */
    protected ?TimerInterface $pausedOnMemoryPressureTimeout = null;

    /** @var int Counter of how many times we have hit the memory HIGH WATERMARK during execution, a high number suggested that memory resources or limit should be increased */
    protected int $pausedOnMemoryPressureCount = 0;

    /** @var bool Flag to track if we are shutting down the engine */
    protected bool $shuttingDown = false;

    /** @var TimerInterface|null */
    protected ?TimerInterface $shutdownTimer = null;

    /**
     * @param string[] $rules An array of Rules defined by classNames
     */
    public function __construct(array $rules)
    {
        $this->rules = $rules;
        $this->logger = new NullLogger();
        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);
    }

    /**
     * @param string|int $id
     * @param string $cmd
     * @param string|null $wd
     * @param array $env
     */
    public function register_input_process($id, string $cmd, ?string $wd = null, array $env = [])
    {
        $this->input_processes_config[$id] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env];
    }

    /**
     * @param Process $process
     * @param int|string $id
     */
    public function setup_input_process(Process $process, $id) {
        try {
            $process->start($this->loop);
        } catch (RuntimeException $exception) {
            $this->logger->critical("Failed to start input process {id}", ['id' => $id, 'exception' => $exception,]);
            return;
        }
        $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

        /**
         * Handle RPC call from the input process
         */
        $process_decoded_stdout->on('data', function(JsonRpcNotification $rpc) use ($id) {
            switch ( $rpc->getMethod() ) {
                case self::INPUT_ACTION_HANDLE:
                    $eventData = $rpc->getParam('event');
                    if (!is_array($eventData)) {
                        $this->logger->critical("Input process did not give us an event array to handle. Received type: {type}, value: {value} ", ['type' => gettype($eventData), 'value' => json_encode($eventData),]);
                        return;
                    }
                    $event = new Event($rpc->getParam('event'));
                    /**
                     * Pass the event to the engine to be handled
                     */
                    $this->engine->handle($event);

                    /**
                     * If we are running in real time then schedule a timer for the next timeout
                     */
                    if ($this->engine->isRealtime()) {
                        $this->scheduleNextTimeout();
                    }
                    break;
                case self::INPUT_ACTION_CHECKPOINT:
                    $this->input_processes_checkpoints[$id] = $rpc->getParams();
                    $this->dirty = true;
                    break;
                case self::RPC_PROCESS_LOG:
                    //Log action expects logLevel to match \Psr\Log\LogLevel
                    $this->logger->log($rpc->getParam('logLevel'), $rpc->getParam('message'));
                    break;
                default:
                    throw new RuntimeException("Unknown json rpc command {$rpc->getMethod()} from input process");
            }
        });

        /**
         * Log any errors we receive on the processed STDERR, error is a fatal event and the stream will be closed, so we need to terminate the process since it can no longer communicate with us
         */
        $process_decoded_stdout->on('error', function(Exception $error) use ($id, $process) {
            $this->logger->error("{id}", ['id'=> $id, 'exception' => $error,]);
            $process->terminate(SIGTERM);
        });

        /**
         * Input processes STDOUT has closed, if we are not in the process of shutting down then we need to terminate the process since it can no longer communicate with us
         */
        $process->stdout->on('close', function() use ($id, $process) {
            if (!$this->shuttingDown && $process->isRunning()) {
                $this->logger->critical("{id} STDOUT closed unexpectedly, terminating process", ['id' => $id,]);
                $process->terminate(SIGTERM);
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
            if ($code === 255) { //255 = PHP Fatal exit code
                $this->logger->critical("Input process $id exit was due to fatal PHP error");
            }
            if ($code !== 0 && false === $this->shuttingDown) {
                /**
                 * @TODO Implement restart of input processes if process terminated with an error and we are not shutting down ($this->shuttingDown)
                 */
            }
            /** We stop processing if there are no input processes available **/
            if (0 === count($this->input_processes)) {
                $this->logger->info("No more input processes running. Shutting down");
                $this->shutdown();
            }
        });
    }

    /**
     * Null will unset calling any Action for emitted events
     * @param string|null $actionName
     */
    public function setNewEventAction(?string $actionName)
    {
        $this->newEventAction = $actionName;
    }

    /**
     * @param string $name
     * @param string $cmd
     * @param string|null $wd
     * @param bool|null $singleShot
     * @param array $env
     */
    public function register_action(string $name, string $cmd, ?string $wd = null, ?bool $singleShot = false, array $env = [])
    {
        $this->actionConfig[$name] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env, 'singleShot' => $singleShot];
    }

    /**
     * @param string $actionName
     * @return Process
     */
    public function start_action(string $actionName): Process
    {
        $actionConfig = $this->actionConfig[$actionName];
        /** Handle singleShot processes true === $actionConfig['singleShot'] ||  */
        if (!isset($this->runningActions[$actionName])) {
            /** If there is no running action then we initialise the process, we call exec to ensure actions can receive our signals and not the default bash wrapper */
            $process = new Process('exec ' . $actionConfig['cmd'], $actionConfig['wd'], $actionConfig['env']);
            $process->start($this->loop);

            $process->stderr->on('data', function ($data) use ($actionName) {
                $this->logger->error("$actionName message: " . trim($data));
            });

            $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

            /** Handler for the Json RPC response */
            $process_decoded_stdout->on('data', function ($rpc) {
                if ($rpc instanceof JsonRpcResponse) {
                    if ($rpc->isSuccess()) {
                        /** Once the action has been processed successfully we can discard of our copy of it */
                        unset($this->inflightActionCommands[$rpc->getId()]);
                    } else {
                        /** Transfer the action from the running queue to the errored queue
                         * @TODO We need to watch this queue and handle any run-away errors (eg a database been unavailable to ingest events)
                         * @TODO This should be put into a function as we call it both here and when an action terminates unexpectedly
                         */
                        $error = [
                            'error' => $rpc->getError(),
                            'action' => $this->inflightActionCommands[$rpc->getId()]['action'],
                        ];
                        $this->erroredActionCommands[] = $error;
                        unset($this->inflightActionCommands[$rpc->getId()]);

                        $this->logger->error($rpc->getError()->getMessage() . " : " . json_encode($rpc->getError()->getData()));
                    }
                    /** Release memory used by the inflight action table */
                    if (count($this->inflightActionCommands) === 0) {
                        $this->inflightActionCommands = [];
                    }
                    $this->dirty = true;
                } elseif ($rpc instanceof JsonRpcNotification) {
                    if ($rpc->getMethod() === self::RPC_PROCESS_LOG) {
                        //Log action expects logLevel to match \Psr\Log\LogLevel
                        $this->logger->log($rpc->getParam('logLevel'), $rpc->getParam('message'));
                    }
                }
            });

            $process->on('exit', function ($code, $term) use ($actionName, $process) {
                /** Action has terminated. If it successfully completed then it will have sent an ack on stdout first before exit */
                if ($term === null) {
                    $level = ($code === 0) ? LogLevel::INFO : LogLevel::ERROR;
                    $this->logger->log($level, "Action {actionName} exited with code: {code}", ['actionName' => $actionName, 'code' => $code]);
                } else {
                    $this->logger->info("Action $actionName exited on signal: $term");
                }
                if ($code === 255) { //255 = PHP Fatal exit code
                    $this->logger->critical("Action $actionName exit was due to fatal PHP error");
                }
                if ($code !== 0)
                {
                    /** Go through inflight actions and look for any that match our exiting with error action. Mark them as errored, otherwise they stay in the inflight action commands queue */
                    $pid = $process->getPid();
                    if (null !== $pid)
                    {
                        $terminatedActions = array_filter($this->inflightActionCommands, function($action) use ($pid) { return $pid === $action['pid']; } );
                        foreach($terminatedActions as $rpcId => $action) {
                            $error = [
                                'error' => "Action process terminated unexpectedly with code $code",
                                'action' => $action['action'],
                            ];
                            $this->erroredActionCommands[] = $error;
                            unset($this->inflightActionCommands[$rpcId]);
                        }
                        unset($terminatedActions);
                    }
                }
                unset($this->runningActions[$actionName]);
                if (count($this->runningActions) === 0)
                {
                    if (true === $this->shuttingDown) {
                        /** If we are shutting down then continue the process */
                        $this->exit();
                    }
                    /** The runningActions queue array can grow large, using a lot of memory,
                     * once it empties we then re-initialise it so that PHP GC can release memory held by the previous array
                     */
                    $this->runningActions = [];
                }
                $this->dirty = true;
            });

            $this->runningActions[$actionName] = $process;
            $this->dirty = true;
            return $process;
        }

        return $this->runningActions[$actionName];
    }

    /**
     * @param string $filename
     */
    public function setSavefileName(string $filename)
    {
        $this->saveFileName = $filename;
    }

    /**
     *
     */
    public function setup_save_state()
    {
        /**
         * Set up a time to save the state of the correlation engine every saveStateSeconds
         */
        $filesystem = Filesystem::create($this->loop);
        $this->saveHandler = $this->loop->addPeriodicTimer($this->saveStateSeconds, function() use ($filesystem) {
            if (($this->engine->isDirty() || $this->dirty) && false === $this->asyncSaveInProgress)
            {
                /** Clear the dirty flags before calling the async save process.
                 * This ensures that changes that occur between now and the save file
                 * being written are scheduled to be flushed on the next cycle
                 */
                $this->engine->clearDirtyFlag();
                $this->dirty = false;
                $this->asyncSaveInProgress = true;
                $this->saveStateAsync($filesystem);
            }
        });

        /** Setup an hourly time to save state (or skip if we are already saving state when this timer fires) */
        $this->loop->addPeriodicTimer(3600, function() use ($filesystem) {
            if (false === $this->asyncSaveInProgress) {
                $this->asyncSaveInProgress = true;
                $this->saveStateAsync($filesystem);
            }
        });
    }

    /**
     * @return array
     */
    public function buildState(): array
    {
        return [
            'engine' => $this->engine->getState(),
            'scheduler' => $this->getState(),
        ];
    }

    /**
     * @param float $seconds
     */
    public function setSaveStateInterval(float $seconds) {
        $this->saveStateSeconds = $seconds;
    }

    /**
     * Save the current system state in an async manner
     * @param FilesystemInterface $filesystem
     */
    public function saveStateAsync(FilesystemInterface $filesystem)
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        if (false === $filename) {
            $this->logger->critical("Error creating temporary save state file, check filesystem");
            return;
        }
        $file = $filesystem->file($filename);
        $file->putContents(json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION))->then(function () use ($file) {
            $file->rename($this->saveFileName)->then(function (\React\Filesystem\Node\FileInterface $newfile) {
                //Everything Good
                $this->asyncSaveInProgress = false;
            });
        }, function (Exception $ex) use($filename) {
            $this->dirty = true; /** We didn't save state correctly, so we mark the scheduler as dirty to ensure it is attempted again */
            $this->asyncSaveInProgress = false;
            if (file_exists($filename)) {
                unlink($filename);
            }
            $this->logger->critical("Save state async failed.", ['exception' => $ex]);
        });
    }

    /**
     * Save the current state in a synchronous manner
     */
    public function saveStateSync()
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        if (false === $filename) {
            $this->logger->critical("Error creating temporary save state file, check filesystem");
            return;
        }
        $state = json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
        if (!(@file_put_contents($filename, $state) === strlen($state) && rename($filename, $this->saveFileName))) {
            $this->logger->critical("Save state sync failed. {lasterror}", ['lasterror' => json_encode(error_get_last())]);
            return;
        }

        $this->logger->debug('State saved to filesystem');
    }

    /**
     * Scheduling timeouts is only supported when the engine is running in live mode. The Correlation engine will check timeouts for batch mode within the handle() function
     * @throws Exception
     */
    function scheduleNextTimeout()
    {
        /**
         * Cancel current timeout and setup the next
         */
        if (null !== $this->nextTimer) {
            $this->loop->cancelTimer($this->nextTimer);
            $this->nextTimer = null;
            $this->dirty = true;
        }

        //Do not schedule any timeout if we are in the process of shutting down
        if (true === $this->shuttingDown) {
            return;
        }

        $timeouts = $this->engine->getTimeouts();
        if (!empty($timeouts)) {
            $nextTimeout = $timeouts[array_key_first($timeouts)];
            $now = new DateTimeImmutable();
            $difference = $nextTimeout['timeout']->getTimestamp() - $now->getTimestamp();

            //If timeout has already past then manually check, this may block if we have fallen behind
            if ($difference <= 0) {
                $this->engine->checkTimeouts(new DateTimeImmutable());
                $this->scheduleNextTimeout();
            } else {
                $this->nextTimer = $this->loop->addTimer($difference, function (){
                    /** We only set a timer for the next one required, however we many have a few to process at the
                     * same time. Use the check timeouts function to process any and all timeouts.
                     */
                    $this->engine->checkTimeouts(new DateTimeImmutable());
                    $this->scheduleNextTimeout();
                });
                $this->timerScheduledAt = new DateTimeImmutable();
                $this->dirty = true;
            }
        }
    }

    /**
     * Boot up from a saved state file
     */
    protected function restoreState()
    {
        /** Load State from save file */
        $savedState = $this->loadStateFromFile();
        $restoring = $savedState !== false;

        if ($restoring) {
            try {
                $this->setState($savedState['scheduler']);
                $this->engine->setState($savedState['engine']);
            } catch (Exception $ex) {
                $this->logger->emergency("A fatal exception was thrown while loading previous saved state.", ['exception' => $ex]);
                exit(-1);
            }
            $this->logger->debug("Successfully loaded from saved state");
        }
        unset($savedState);

        /** Force a run of the PHP GC and release caches. This helps clearing out memory consumed by restoring state from a large json file */
        gc_collect_cycles();
        gc_mem_caches();

        /** Inject some synthetic events in to the engine to flag that the engine is starting for the first time or restoring
         * Rules can handle these events for initialisation purposes (handy for setting up rules that detect when an event is missing)
         */
        $this->loop->futureTick(function() use ($restoring) {
            $event = $restoring ? new Event(['event' => static::CONTROL_MSG_RESTORED_STATE]) : new Event(['event' => static::CONTROL_MSG_NEW_STATE]);

            /**
             * Pass the event to the engine to be handled
             */
            $this->engine->handle($event);

            /**
             * If we are running in real time then schedule a timer for the next timeout
             */
            if ($this->engine->isRealtime()) {
                $this->scheduleNextTimeout();
            }
        });
    }

    /**
     * Start the Engine
     */
    public function run()
    {
        if (!$this->isOpcacheEnabled())
        {
            $this->logger->warning("Opcache is not enabled. This will reduce performance and increase memory usage");
        }

        $this->loop = Loop::get();
        $this->logger->debug("Using event loop implementation: {class}", ['class' => get_class($this->loop)]);

        /** Restore the state of the scheduler and engine */
        $this->restoreState();

        /** Start input processes */
        foreach ($this->input_processes_config as $id => $config)
        {
            $env = $config['env'];
            /** If we have a checkpoint in our save state pass this along to the input process via the ENV */
            if (isset($this->input_processes_checkpoints[$id]))
            {
                $env = array_merge([static::CHECKPOINT_VARNAME => json_encode($this->input_processes_checkpoints[$id])], $env);
            }
            //Use exec to ensure process receives our signals and not the bash wrapper
            $process = new Process('exec ' . $config['cmd'], $config['wd'], $env);
            $this->setup_input_process($process, $id);
            if ($process->isRunning()) {
                $this->logger->info("Started input process $id");
                $this->input_processes[$id] = $process;
            } else {
                $this->logger->emergency("An input process failed to start, exiting");
                exit(1);
            }
        }

        /**
         * An event has been emitted by a Rule
         * If the Scheduler has been configured to persist new events via setting newEventAction then we wrap this in the defined Action and emit it via the Engine,
         * Otherwise we send the new event back through the Correlation Engine as an Event and let it handle it per the defined Rules
         */
        $this->engine->on('event', function(Event $event) {
            if (null === $this->newEventAction) {
                $this->loop->futureTick(function() use ($event) {
                    /** We handle the new event in a future tick to ensure we don't get stuck in a loop if a timed out Rule is emitted an event */
                    $this->engine->handle($event);
                });
            } else {
                $action = new Action($this->newEventAction, $event);
                $this->engine->emit('action', [$action]);
            }
        });

        /** Handle request to run an action */
        $this->engine->on('action', function(Action $action) {
            $actionName = $action->getCmd();
            if (isset($this->actionConfig[$actionName]))
            {
                $process = $this->start_action($actionName);
                /** Once the process is up and running we then write out our data via it's STDIN, encoded as a JSON RPC call */
                /** @TODO id should be a sequential number and not generated from mt_rand() */
                $rpc_request = new JsonRpcRequest(self::ACTION_RUN_METHOD, $action->getVars(), mt_rand());
                $this->inflightActionCommands[$rpc_request->getId()]['action'] = $action;
                $this->inflightActionCommands[$rpc_request->getId()]['pid'] = $process->getPid();
                $this->dirty = true;
                $process->stdin->write(json_encode($rpc_request) . "\n");
            }
            else
            {
                $this->logger->error("Unable to start unknown action " . json_encode($action));
            }
        });

        /** Initialise the state saving task */
        $this->setup_save_state();

        /** Monitor memory usage */
        $this->memoryLimit = $this->calculateMemoryLimit();
        $this->logger->debug("Memory limit set to {bytes} Bytes", ['bytes' => $this->memoryLimit,]);
        $this->loop->addPeriodicTimer(2, function() { $this->checkMemoryPressure(); });

        /** Gracefully shutdown */
        // ctrl+c
        $this->loop->addSignal(SIGINT, function() {
            $this->logger->debug( "received SIGINT scheduling shutdown...");
            $this->shutdown();
        });
        // kill
        $this->loop->addSignal(SIGTERM, function() {
            $this->logger->debug("received SIGTERM scheduling shutdown...");
            $this->shutdown();
        });
        // logout
        $this->loop->addSignal(SIGHUP, function() {
            /** If we receive a HUP save the current running state. Don't exit
             * Force a run of the PHP GC and release caches.
             */
            $this->logger->debug("SIGHUP received, clearing caches and saving non-dirty state");
            gc_collect_cycles();
            gc_mem_caches();
            $this->saveStateSync();
        });

        /** GO! */
        $this->loop->run();
    }

    /**
     * Initialise shutdown by stopping processes and timers
     */
    public function shutdown() {
        $this->shuttingDown = true;
        if (null !== $this->nextTimer) {
            $this->loop->cancelTimer($this->nextTimer);
            $this->nextTimer = null;
        }

        if (null !== $this->saveHandler) {
            $this->loop->cancelTimer($this->saveHandler);
        }

        if (count($this->input_processes) > 0) {
            $this->logger->info( "Shutting down running input processes");
            foreach ($this->input_processes as $processKey => $process) {
                if (false === $process->terminate(SIGTERM)) {
                    $this->logger->error( "Unable to send SIGTERM to input process $processKey");
                }
                if ($process->isStopped()) {
                    $process->terminate(SIGCONT); // If our input processes are paused by memory pressure then we need to send SIGCONT after SIGTERM as they are currently stopped
                }
            }

            //Set up a timer to forcefully move the scheduler into the next shutdown phase if the input's have not shutdown in time
            $this->shutdownTimer = $this->loop->addTimer(10.0, function() {
                $this->logger->warning( "Input processes did not shutdown within the timeout delay...");
                $this->stop();
            });
        } else {
            $this->stop();
        }

    }

    /**
     * Input processes are stopped. Shutdown any running actions (some may need to be flushed)
     */
    public function stop()
    {
        if (null !== $this->shutdownTimer) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }
        /**
         * Notify any rules listening for a Stop event that we are stopping
         */
        $this->engine->handle(new Event(['event' => static::CONTROL_MSG_STOP]));

        /** Check if we have any running action commands, if we do then some actions may not have completed and/or need to flush+complete tasks. Send them a SIGTERM to complete their shutdown */
        if (count($this->runningActions) > 0) {
            $this->logger->info( "Shutting down running action processes");
            foreach ($this->runningActions as $processKey => $process) {
                if (false === $process->terminate(SIGTERM)) {
                    $this->logger->error("Unable to send SIGTERM to action process $processKey");
                }
            }

            //Set up a timer to forcefully stop the scheduler if all processes don't terminate in time
            $this->shutdownTimer = $this->loop->addTimer(10.0, function() {
                $this->logger->warning( "Action processes did not shutdown within the timeout delay...");
                $this->exit();
            });
        } else {
            $this->exit();
        }
    }

    /**
     * Stop the Loop and sync state to disk
     */
    public function exit() {
        if (null !== $this->shutdownTimer) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }
        $this->loop->futureTick(function() {
            if (count($this->inflightActionCommands) > 0 ) {
                $this->logger->error("There were still inflight action commands at shutdown.");
            }
            $this->loop->stop();
            $this->logger->debug("Event Loop stopped");
            $this->saveStateSync(); //Loop is stopped. Do a blocking synchronous save of current state prior to exit.
        });
    }

    /**
     * @return false|array
     */
    public function loadStateFromFile()
    {
        if (file_exists($this->saveFileName))
        {
            $contents = file_get_contents($this->saveFileName);
            if ($contents === false || $contents === '') {
                $this->logger->critical('State file exists but contents could not be read or were empty');
                exit(1);
            }
            $state = json_decode($contents, true);
            if ($state === null) {
                $this->logger->critical('Save state file was corrupted. JSON Error: ' . json_last_error_msg() );
                exit(1);
            }
            return $state;
        }
        return false;
    }

    /**
     * @return array
     */
    protected function getState() : array
    {
        $state = [];
        $state['input']['checkpoints'] = $this->input_processes_checkpoints;
        $state['actions'] = ['inflight' => array_map(function($action) { return $action['action']; }, $this->inflightActionCommands), 'errored' => $this->erroredActionCommands];
        $state['nextTimeout'] = $this->nextTimer === null ? 'none' : $this->timerScheduledAt->modify($this->nextTimer->getInterval() . ' seconds')->format('c');
        $state['inputPaused'] = $this->pausedOnMemoryPressure ? 'Yes' : 'No';
        $state['pausedCount'] = $this->pausedOnMemoryPressureCount;
        $state['memoryPercentageUsed'] = $this->currentMemoryPercentUsed;
        //@TODO add a clean shutdown flag here in save state
        return $state;
    }

    /**
     * @param array $state
     */
    public function setState(array $state)
    {
        $this->input_processes_checkpoints = $state['input']['checkpoints'];
        /** If we had any actions still processing when we last saved state then move those to errored as we don't know if they completed */
        /** @TODO, this could be a big array, we need to handle that in a memory sensitive way */
        /** @TODO - Replay any errored actions, then replay any inflight actions recorded. If these fail then we should thrown an error and exit. Ensure no dataloss */
        if (count($state['actions']['errored']) > 0)
        {
            $this->logger->warning("Failed actions detected from previous execution");
            if (count($state['actions']['errored']) > 50)
            {
                $this->logger->warning("Large number of failed actions. Memory consumption for state table may be large.");
            }
        }
        if (true === static::PRESERVE_FAILED_EVENTS_ONLOAD) {
            $this->erroredActionCommands = array_merge($state['actions']['inflight'], $state['actions']['errored']);
        }
    }

    /**
     * Memory usage can increase rapidly with Rules that are buffering data, and a large number of inflight Action Commands (specifically the state table tracking their execution, or failure).
     * Compare our memory usage against the limit set for the PHP process and a HIGH watermark,
     *  once we go above the high watermark or the number of inflight actions exceeds the running actions watermark,
     *  we pause input processes to allow inflight actions to complete and reduce memory usage.
     */
    protected function checkMemoryPressure()
    {
        if ($this->memoryLimit === 0) {
            $this->memoryLimit = $this->calculateMemoryLimit();
        } elseif ($this->memoryLimit === -1) {
            return; //We are configured for unlimited memory, so we disable memory pressure checks
        }

        $current_memory_usage = memory_get_usage();

        $percent_used = (int)(($current_memory_usage / $this->memoryLimit) * 100);

        /** Try releasing memory first and recalculate percentage used */
        if ($this->pausedOnMemoryPressure || $percent_used >= static::MEMORY_PRESSURE_HIGH_WATERMARK) {
            /** Running this every check cycle negatively impacts the scheduler's performance,
             *  however since we are paused (or going to pause) at this stage, and are awaiting the external action processes to complete the actual impact will be minimal
             */
            gc_collect_cycles();
            gc_mem_caches();
            $current_memory_usage = memory_get_usage();
            $percent_used = (int)(($current_memory_usage / $this->memoryLimit) * 100);
        }

        $this->currentMemoryPercentUsed = $percent_used;

        if (false === $this->pausedOnMemoryPressure &&
                ($percent_used >= static::MEMORY_PRESSURE_HIGH_WATERMARK ||
                count($this->inflightActionCommands) > static::RUNNING_ACTION_LIMIT_HIGH_WATERMARK)
        )
        {
            $this->logger->warning(
                "Currently using $percent_used% of memory limit with " . count($this->inflightActionCommands) . " inflight actions. Pausing input processes");

            foreach($this->input_processes as $processId => $process)
            {
                if ($process->isRunning()) {
                    $process->terminate(SIGSTOP);
                    $this->logger->debug("Paused input process {id}", ['id' => $processId,]);
                }
            }
            $this->pausedOnMemoryPressure = true;
            ++$this->pausedOnMemoryPressureCount;

            /** @TODO take into account delaying shutdown if we still have some outstanding actions and memory usage is dropping */
            $this->pausedOnMemoryPressureTimeout = $this->loop->addTimer(300, function() {
                if ($this->pausedOnMemoryPressure) {
                    $this->logger->critical("Timeout! Input processes are still paused, shutting down");
                    $this->shutdown();
                }
            });
        }
        else
        {
            if ($this->pausedOnMemoryPressure &&
                $percent_used <= static::MEMORY_PRESSURE_LOW_WATERMARK &&
                count($this->inflightActionCommands) < static::RUNNING_ACTION_LIMIT_LOW_WATERMARK) {

                //Cancel the memory pressure timout
                if ($this->pausedOnMemoryPressureTimeout !== null) {
                    $this->loop->cancelTimer($this->pausedOnMemoryPressureTimeout);
                    $this->pausedOnMemoryPressureTimeout = null;
                }
                //Resume input
                foreach ($this->input_processes as $processId => $process) {
                    $process->terminate(SIGCONT);
                    $this->logger->debug( "Resuming input process {id}", ['id' => $processId,]);
                }
                $this->pausedOnMemoryPressure = false;
            }
        }
    }

    /**
     * @return int
     * Calculate in Bytes the memory limit set in the PHP configuration
     */
    protected function calculateMemoryLimit() : int
    {
        $multiplierTable = ['K' => 1024, 'M' => 1024**2, 'G' => 1024**3];

        $memory_limit_setting = ini_get('memory_limit');

        if ("-1" == $memory_limit_setting) {
            return -1;
        }

        preg_match("/^(-?[.0-9]+)([KMG])?$/i", $memory_limit_setting, $matches, PREG_UNMATCHED_AS_NULL);

        $bytes = (int)$matches[1];
        $multiplier = (null == $matches[2]) ? 1 : $multiplierTable[strtoupper($matches[2])];

        return $bytes * $multiplier;
    }

    /**
     * @return bool
     */
    protected function isOpcacheEnabled() : bool
    {
        if (function_exists('opcache_get_status'))
        {
            $status = opcache_get_status(false);

            return (false !== $status);
        }
        return false;
    }
}
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
use EdgeTelemetrics\EventCorrelation\Management\Server;
use EdgeTelemetrics\EventCorrelation\SaveHandler\FileAdapter;
use EdgeTelemetrics\EventCorrelation\SaveHandler\SaveHandlerInterface;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
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
use RuntimeException;

use function array_filter;
use function array_key_first;
use function array_keys;
use function array_map;
use function array_shift;
use function function_exists;
use function gettype;
use function hrtime;
use function is_array;
use function opcache_get_status;
use function json_encode;
use function memory_get_usage;
use function count;
use function get_class;
use function gc_collect_cycles;
use function gc_mem_caches;
use function array_merge;
use function mt_rand;
use function trim;

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

    /** @var float hrtime for when the scheduler is started */
    protected float $schedulerStartTime;

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
     * @var TimerInterface[] Scheduled tasks for the scheduler
     */
    protected array $scheduledTasks = [];

    /** @var SaveHandlerInterface */
    protected SaveHandlerInterface $saveStateHandler;

    /**
     * @var string Filename to save state to
     */
    protected string $saveFileName = '/tmp/php-ec-savepoint';

    /**
     * @var bool Flag if the scheduler has information that needs to be flushed to the save file.
     */
    protected bool $dirty;

    /**
     * @var float How ofter to save state
     */
    protected float $saveStateSeconds = 1;

    /**
     * Flag for whether we keep failed actions when loading or drop them.
     */
    const PRESERVE_FAILED_EVENTS_ONLOAD = true;

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

    /** @var string[] Accepted input RPC methods */
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

    /**
     * @var bool Flag if we are replaying failed actions
     */
    protected bool $errorRecovery = false;

    /** @var bool Flag to track if we are shutting down the engine */
    protected bool $shuttingDown = false;

    /** @var TimerInterface|null */
    protected ?TimerInterface $shutdownTimer = null;

    /** @var bool */
    protected bool $enabledManagementServer = false;

    /** @var null|Server */
    protected ?Server $managementServer = null;

    /**
     * @param array<class-string<IEventMatcher>> $rules An array of Rules defined by classNames
     */
    public function __construct(array $rules)
    {
        $this->schedulerStartTime = hrtime(true);
        $this->rules = $rules;
        $this->logger = new NullLogger();
        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);
    }

    /**
     * @param string|int $id Descriptive and unique key of the input process
     * @param string $cmd Command to execute //@TODO PHP7.4 allows command to be provided as an array
     * @param string|null $wd Working directory for the process, defaults to current PHP working directory of the Scheduler
     * @param array $env Key value pair of Environmental variables to pass to the process
     * @param bool $essential If true, Scheduler will shut everything down if this input process exit with no errorCode or doesn't exist
     */
    public function register_input_process($id, string $cmd, ?string $wd = null, array $env = [], bool $essential = false) : void
    {
        $this->input_processes_config[$id] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env, 'essential' => $essential];
    }

    /**
     * @return void
     */
    public function initialise_input_processes() : void {
        $this->logger->debug('Initialising input processes');
        foreach (array_keys($this->input_processes_config) as $id) {
            try {
                $this->start_input_process($id);
            } catch (RuntimeException $ex) {
                $this->logger->emergency("An input process failed to start during initialisation.", ['exception' => $ex]);
                exit(1);
            }
        }
    }

    /**
     * @param string|int $id
     * @return Process
     */
    public function start_input_process($id) : Process {
        if (isset($this->input_processes[$id])) {
            $this->logger->critical('Input process ' . $id . ' already running');
            return $this->input_processes[$id];
        }
        $config = $this->input_processes_config[$id];
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
            return $process;
        }
        throw new RuntimeException('Input process ' . $id . ' failed to start');
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
            if ($code === 127) {
                $this->logger->critical("Input process $id exit was due to Command Not Found");
            } elseif ($code === 255) { //255 = PHP Fatal exit code
                $this->logger->critical("Input process $id exit was due to fatal PHP error");
            }
            /** Restart the input process if it exits with an error code EXCEPT if it is exit code 127 - Command Not Found */
            if (false === $this->shuttingDown) {
                if (!in_array($code, [0, 127], true)) {
                    $this->logger->debug("Restarting process $id");
                    $this->start_input_process($id);
                } elseif ($this->input_processes_config[$id]['essential']) {
                    if ($code === 127) {
                        $this->logger->info("Essential input process cannot be found. Shutting down");
                    } else {
                        $this->logger->info("Essential input process has stopped cleanly. Shutting down");
                    }
                    $this->shutdown();
                    return;
                }
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
     * @noinspection PhpUnused
     */
    public function setNewEventAction(?string $actionName) : void
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
    public function register_action(string $name, string $cmd, ?string $wd = null, ?bool $singleShot = false, array $env = []) : void
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
            $process = new Process('exec ' . $actionConfig['cmd'], $actionConfig['wd'], $actionConfig['env'] ?? []);
            $process->start($this->loop);

            $this->logger->info("Started action process $actionName");

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

                        if ($this->errorRecovery === true) {
                            $this->logger->critical('An action process failed again during recovery');
                            $this->shutdown();
                        }
                    }
                    /** Release memory used by the inflight action table */
                    if (count($this->inflightActionCommands) === 0) {
                        $this->inflightActionCommands = [];

                        if ($this->errorRecovery === true) {
                            $this->logger->info('Replay of errored actions completed successfully. Resuming normal operations');
                            $this->errorRecovery = false;
                            $this->initialise_input_processes();
                        }
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
                if ($code === 127) {
                    $this->logger->critical("Action $actionName exit was due to Command Not Found");
                } elseif ($code === 255) { //255 = PHP Fatal exit code
                    $this->logger->critical("Action $actionName exit was due to fatal PHP error");
                }
                if ($code !== 0)
                {
                    /** Go through inflight actions and look for any that match our exiting with error action. Mark them as errored, otherwise they stay in the inflight action commands queue */
                    $pid = $process->getPid();
                    if (null !== $pid)
                    {
                        $terminatedActions = array_filter($this->inflightActionCommands, function($action) use ($pid) { return $pid === $action['pid']; } );
                        $terminatedMessage =  "Action process terminated unexpectedly " . (($term === null) ? "with code: $code" : "on signal: $term");
                        foreach($terminatedActions as $rpcId => $action) {
                            $error = [
                                'error' => $terminatedMessage,
                                'action' => $action['action'],
                            ];
                            $this->erroredActionCommands[] = $error;
                            unset($this->inflightActionCommands[$rpcId]);
                        }
                    }

                    if ($this->errorRecovery === true) {
                        $this->logger->critical('An action process failed again during recovery');
                        $this->shutdown();
                    }
                }
                unset($this->runningActions[$actionName]);
                if (count($this->runningActions) === 0)
                {
                    /**
                     * The runningActions queue array can grow large, using a lot of memory,
                     * once it empties we then re-initialise it so that PHP GC can release memory held by the previous array
                     */
                    $this->runningActions = [];

                    if (true === $this->shuttingDown && count($this->input_processes) === 0) {
                        /** If we are shutting down and all input processes have stopped then continue the shutdown process straight away instead of waiting for the timers */
                        $this->exit();
                    }
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
    public function setSavefileName(string $filename) : void
    {
        $this->saveFileName = $filename;
    }

    /**
     *
     */
    public function setup_save_state() : void
    {
        /**
         * Set up a time to save the state of the correlation engine every saveStateSeconds
         */
        $this->saveStateHandler->on('save:failed', function($arguments) {
            $this->dirty = true;
            if ($this->shuttingDown === false) { //Failure is expected if the save handler is running when the scheduler starts shutting down. A sync save state will be run at end of shutdown
                $this->logger->critical("Save state async failed.", ['exception' => $arguments['exception']]);
            }
        });

        $this->scheduledTasks[] = $this->loop->addPeriodicTimer($this->saveStateSeconds, function() {
            if (($this->engine->isDirty() || $this->dirty) && false === $this->saveStateHandler->asyncSaveInProgress())
            {
                /** Clear the dirty flags before calling the async save process.
                 * This ensures that changes that occur between now and the save file
                 * being written are scheduled to be flushed on the next cycle
                 */
                $this->engine->clearDirtyFlag();
                $this->dirty = false;
                $this->saveStateHandler->saveStateAsync($this->buildState());
            }
        });

        /** Set up an hourly time to save state (or skip if we are already saving state when this timer fires) */
        $this->scheduledTasks[] = $this->loop->addPeriodicTimer(3600, function() {
            if (false === $this->saveStateHandler->asyncSaveInProgress()) {
                $this->saveStateHandler->saveStateAsync($this->buildState());
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
    public function setSaveStateInterval(float $seconds) : void {
        $this->saveStateSeconds = $seconds;
    }

    /**
     * Scheduling timeouts is only supported when the engine is running in live mode. The Correlation engine will check timeouts for batch mode within the handle() function
     * @throws Exception
     */
    function scheduleNextTimeout() : void
    {
        /**
         * Cancel current timeout and set up the next one
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
    protected function restoreState() : void
    {
        /** Load State from save file */
        try {
            $savedState = $this->saveStateHandler->loadState();
        } catch (RuntimeException $ex) {
            $this->logger->critical($ex->getMessage());
            exit(1);
        }
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

        /** Force a run of the PHP GC and release caches. This helps clear out memory consumed by restoring state from a large json file */
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
    public function run() : void
    {
        if (!$this->isOpcacheEnabled())
        {
            $this->logger->warning("Opcache is not enabled. This will reduce performance and increase memory usage");
        }

        $this->loop = Loop::get();
        $this->logger->debug("Using event loop implementation: {class}", ['class' => get_class($this->loop)]);

        /** Initialise the management server early in the startup */
        $this->initialiseManagementServer();

        /** Restore the state of the scheduler and engine */
        $this->saveStateHandler = new FileAdapter('/tmp', $this->saveFileName, $this->logger, $this->loop);
        $this->restoreState();

        /**
         * An event has been emitted by a Rule
         * If the Scheduler has been configured to persist new events via setting newEventAction then we wrap this in the defined Action and emit it via the Engine,
         * Otherwise we send the new event back through the Correlation Engine as an Event and let it handle it per the defined Rules
         */
        $this->engine->on('event', function(Event $event) {
            if (null === $this->newEventAction) {
                $this->loop->futureTick(function() use ($event) {
                    /** We handle the new event in a future tick to ensure we don't get stuck in a loop if a timed out Rule is emitting an event */
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

        /** If we have any errored actions then we replay them and attempt recovery. In normal state we initialise the input processes */
        if (count($this->erroredActionCommands)) {
            $this->logger->notice('Beginning failed action recovery process');
            $this->errorRecovery = true;
            while(count($this->erroredActionCommands) > 0) {
                $errored = array_shift($this->erroredActionCommands);
                $action = new Action($errored['action']['cmd'], $errored['action']['vars']);
                $this->engine->emit('action', [$action]);
            }
            $this->erroredActionCommands = [];
        } else {
            $this->initialise_input_processes();
        }

        /** Initialise the state saving task */
        $this->setup_save_state();

        /** Monitor memory usage */
        $sysInfo = new SysInfo();
        $this->memoryLimit = $sysInfo->getMemoryLimit();
        $allowable = $sysInfo->getAllowableMemoryLimit();
        $percentage = $this->memoryLimit === -1 ? 100 : ($this->memoryLimit/$allowable)*100;
        $this->logger->debug("Memory limit set to {bytes} Bytes {percent}% of {total} Total Allowable", ['bytes' => $this->memoryLimit, 'total' => $allowable, 'percent' => number_format($percentage, 2)]);
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
            $this->saveStateHandler->saveStateSync($this->buildState());
        });

        /** GO! */
        $this->loop->run();
    }

    /**
     * Initialise shutdown by stopping processes and timers
     */
    public function shutdown() : void {
        $this->shuttingDown = true;
        if (null !== $this->nextTimer) {
            $this->loop->cancelTimer($this->nextTimer);
            $this->nextTimer = null;
        }

        while ($task = array_pop($this->scheduledTasks)) {
            $this->loop->cancelTimer($task);
        }

        if (count($this->input_processes) > 0) {
            $this->logger->debug( "Shutting down running input processes");
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
    public function stop() : void
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
            $this->loop->futureTick(function() {
                $this->logger->debug("Shutting down running action processes");
                foreach ($this->runningActions as $processKey => $process) {
                    if (false === $process->terminate(SIGTERM)) {
                        $this->logger->error("Unable to send SIGTERM to action process $processKey");
                    }
                }

                //Set up a timer to forcefully stop the scheduler if all processes don't terminate in time
                $this->shutdownTimer = $this->loop->addTimer(10.0, function () {
                    $this->logger->warning("Action processes did not shutdown within the timeout delay...");
                    $this->exit();
                });
            });
        } else {
            $this->exit();
        }
    }

    /**
     * Stop the Loop and sync state to disk
     */
    public function exit() : void {
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
            $this->saveStateHandler->saveStateSync($this->buildState()); //Loop is stopped. Do a blocking synchronous save of current state prior to exit.
        });
    }

    /**
     * @return array
     */
    protected function getState() : array
    {
        $state = [];
        $state['uptime_msec'] = (int)round((hrtime(true) - $this->schedulerStartTime)/1e+6);
        $state['input']['running'] = array_keys($this->input_processes);
        $state['input']['checkpoints'] = $this->input_processes_checkpoints;
        $state['actions'] = ['inflight' => array_map(function($action) { return $action['action']; }, $this->inflightActionCommands), 'errored' => $this->erroredActionCommands];
        $state['nextTimeout'] = $this->nextTimer === null ? 'none' : $this->timerScheduledAt->modify($this->nextTimer->getInterval() . ' seconds')->format('c');
        $state['inputPaused'] = $this->pausedOnMemoryPressure ? 'Yes' : 'No';
        $state['pausedCount'] = $this->pausedOnMemoryPressureCount;
        $state['memoryPercentageUsed'] = $this->currentMemoryPercentUsed;
        $state['saveFileSizeBytes'] = $this->saveStateHandler->lastSaveSizeBytes();
        $state['saveStateLastDuration'] = $this->saveStateHandler->lastSaveWriteDuration();
        //@TODO add a clean shutdown flag here in save state
        return $state;
    }

    /**
     * @param array $state
     */
    public function setState(array $state) : void
    {
        $this->input_processes_checkpoints = $state['input']['checkpoints'];
        /** If we had any actions still processing when we last saved state then move those to errored as we don't know if they completed */
        /** @TODO, this could be a big array, we need to handle that in a memory sensitive way */
        $erroredCount = count($state['actions']['errored']);
        if ($erroredCount > 0)
        {
            $this->logger->warning("$erroredCount failed actions detected from previous execution");
            if ($erroredCount > 50)
            {
                $this->logger->warning("Large number of failed actions. Memory consumption for state table may be large.");
            }
        }
        if (true === static::PRESERVE_FAILED_EVENTS_ONLOAD) {
            $this->erroredActionCommands = $state['actions']['errored'];
            unset($state['actions']['errored']);
            while(count($state['actions']['inflight'])) {
                $inflight = array_shift($state['actions']['inflight']);
                $this->erroredActionCommands[] = [
                    'error' => 'Inflight when process exited',
                    'action' => $inflight,
                ];
            }
        }
    }

    /**
     * Memory usage can increase rapidly with Rules that are buffering data, and a large number of inflight Action Commands (specifically the state table tracking their execution, or failure).
     * Compare our memory usage against the limit set for the PHP process and a HIGH watermark,
     *  once we go above the high watermark or the number of inflight actions exceeds the running actions watermark,
     *  we pause input processes to allow inflight actions to complete and reduce memory usage.
     */
    protected function checkMemoryPressure() : void
    {
        if ($this->memoryLimit === 0) {
            $sysInfo = new SysInfo();
            $this->memoryLimit = $sysInfo->getMemoryLimit();
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

    /**
     * Enable or disable the experimental management server
     * @param bool $enable
     */
    public function enableManagementServer(bool $enable) {
        $this->enabledManagementServer = $enable;
    }

    /**
     * Initialise the management server if enabled
     */
    protected function initialiseManagementServer() {
        if ($this->enabledManagementServer) {
            $this->managementServer = new Server($this);
            $this->logger->info("Management server listening on " . ($this->managementServer->getListeningAddress() ?? 'Unable to retrieve address'));
        }
    }
}
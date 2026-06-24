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

use EdgeTelemetrics\EventCorrelation\Library\EventLog;
use EdgeTelemetrics\EventCorrelation\Management\Server;
use EdgeTelemetrics\EventCorrelation\SaveHandler\FileAdapter;
use EdgeTelemetrics\EventCorrelation\SaveHandler\SaveHandlerInterface;
use EdgeTelemetrics\EventCorrelation\Scheduler\ActionExecutionCoordinator;
use EdgeTelemetrics\EventCorrelation\Scheduler\Heartbeat;
use EdgeTelemetrics\EventCorrelation\Scheduler\MetricsCollector;
use EdgeTelemetrics\EventCorrelation\Scheduler\SourceExecutionCoordinator;
use EdgeTelemetrics\EventCorrelation\Scheduler\SourceFunction;
use EdgeTelemetrics\EventCorrelation\Scheduler\State;
use EdgeTelemetrics\EventCorrelation\StateMachine\IEventMatcher;
use EdgeTelemetrics\JSON_RPC\Error;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use RuntimeException;

use Throwable;
use function array_keys;
use function array_map;
use function array_pop;
use function array_shift;
use function bin2hex;
use function function_exists;
use function gc_enable;
use function gc_enabled;
use function hrtime;
use function max;
use function memory_get_peak_usage;
use function number_format;
use function opcache_get_status;
use function json_encode;
use function memory_get_usage;
use function count;
use function get_class;
use function gc_collect_cycles;
use function gc_mem_caches;
use function random_bytes;
use function round;

use const SIGINT;
use const SIGTERM;
use const SIGHUP;

/**
 * Class Scheduler
 * @package EdgeTelemetrics\EventCorrelation
 * @property LoggerInterface $logger
 */
class Scheduler implements LoggerAwareInterface {
    const CHECKPOINT_VARNAME = 'PHPEC_CHECKPOINT';

    /** @var float hrtime for when the scheduler is started */
    protected float $schedulerStartTime;

    /** @var State The current state of the scheduler */
    protected readonly State $state;

    /**
     * @var LoopInterface
     */
    protected LoopInterface $loop;

    /**
     * @var CorrelationEngine
     */
    protected CorrelationEngine $engine;

    /**
     * @var string[] Class list of rules
     */
    protected array $rules = [];

    /**
     * Keep track of Action execution
     */
    protected ActionExecutionCoordinator $actionExecutionCoordinator;

    /**
     * Keep track of Source process execution
     */
    protected SourceExecutionCoordinator $sourceExecutionCoordinator;

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
    protected bool $dirty = false;

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

    /**
     * Event type when the heartbeat time goes off
     */
    const CONTROL_MSG_HEARTBEAT = 'PHP-EC:Engine:Heartbeat';

    /**
     * Valid list of control messages
     */
    const CONTROL_MESSAGES = [
        self::CONTROL_MSG_NEW_STATE,
        self::CONTROL_MSG_RESTORED_STATE,
        self::CONTROL_MSG_STOP,
        self::CONTROL_MSG_HEARTBEAT,
    ];

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

    /** @var int Counter of how many times we have hit the memory HIGH WATERMARK during execution, a high number suggested that memory resources or limit should be increased */
    protected int $pausedOnMemoryPressureCount = 0;

    /** @var TimerInterface|null */
    protected ?TimerInterface $shutdownTimer = null;

    /** @var bool */
    protected bool $enabledManagementServer = false;

    /** @var null|Server */
    protected ?Server $managementServer = null;

    /**
     * @var int Number of seconds between heartbeats or 0 to disable
     */
    protected int $heartbeat_seconds = 0;

    /** @var int Seconds to delay before the first heartbeat is sent */
    private int $heartbeat_initialDelay = 0;

    /** @var ?LoggerInterface PSR-3 Logger */
    protected ?LoggerInterface $logger = null;

    /**
     * @var MetricsCollector
     */
    protected MetricsCollector $metricsCollector;

    protected bool $trackRecentEvents = false;

    /**
     * @var EventLog Recent events buffer
     */
    protected EventLog $recentEvents;

    /**
     * Sets a logger.
     */
    public function setLogger(LoggerInterface $logger): void {
        $this->logger = $logger;
        $this->engine->setLogger($logger);
        $this->actionExecutionCoordinator->setLogger($logger);
        $this->sourceExecutionCoordinator->setLogger($logger);
        $this->metricsCollector->setLogger($this->logger);
    }

    /**
     * @param array<class-string<IEventMatcher>> $rules An array of Rules defined by classNames
     */
    public function __construct(array $rules)
    {
        $this->state = new State(State::STARTING);
        $this->loop = Loop::get();
        $this->schedulerStartTime = hrtime(true);
        $this->rules = $rules;
        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);
        $this->initialiseActionExecution();
        $this->initialiseSourceExecution();
        $this->metricsCollector = new MetricsCollector();
        $this->setLogger(new NullLogger());
        $this->recentEvents = new EventLog();

        $this->state->on('scheduler.state.transition', function($new, $old) {
           $this->logger->info("Scheduler state transitioning from $old to $new");
        });
    }

    /**
     * @param string|int $id Descriptive and unique key of the input process
     * @param string|array|SourceFunction $cmd Command to execute
     * @param string|null $wd Working directory for the process, defaults to current PHP working directory of the Scheduler
     * @param array $env Key-value pair describing Environmental variables to pass to the process
     * @param bool $essential If true, Scheduler will shut everything down if this input process exit with no errorCode or doesn't exist
     * @param bool $autostart Whether to start the process automatically on startup or when requested
     */
    public function register_input_process(string|int $id, string|array|SourceFunction $cmd, ?string $wd = null, array $env = [], bool $essential = false, bool $autostart = true) : void
    {
        $this->sourceExecutionCoordinator->register_input_process($id, $cmd, $wd, $env, $essential, $autostart);
    }

    /**
     * @param string|int $id
     * @return void
     */
    public function unregister_input_process(string|int $id) : void
    {
        $this->sourceExecutionCoordinator->unregister_input_process($id);
    }

    /**
     * Get the recent events buffer.
     * @return array
     */
    public function getRecentEvents(): array
    {
        return $this->recentEvents->getEvents();
    }

    protected function handleEvent(Event $event): void {
        try {
            $this->engine->handle($event);
        } catch (Throwable $ex) {
            $this->logger->emergency("Rules must not throw exceptions", ['exception' => $ex]);
            $this->panic($ex);
        }
        if ($this->trackRecentEvents) {
            $this->recentEvents->add($event);
        }
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

    protected function handleAction(Action $action) : void {
        $this->actionExecutionCoordinator->handleAction($action);
    }

    protected function initialiseActionExecution() : void {
        $this->actionExecutionCoordinator = $ec = new ActionExecutionCoordinator();

        $ec->on('dirty', function() {
            $this->dirty = true;
        });

        $ec->on('process.start', function($actionName) {
            $this->logger->info("Started action process $actionName");
        });

        $ec->on('action.failed', function($action, $exception) {
            $error = [
                'error' => [
                    'code' => $exception->getCode(),
                    'message' => $exception->getMessage(),
                ],
                'action' => $action,
            ];
            $this->erroredActionCommands[] = $error;

            if ($this->state->state() === State::RECOVERY) {
                $this->logger->critical("Action {$action->getCmd()} failed again during recovery", ['exception' => $exception]);
            }
        });

        $ec->once('action.failed', function($action, $exception) {
            if ($this->state->state() === State::RECOVERY) {
                $this->loop->futureTick(function() {
                    $this->shutdown();
                });
            }
        });

        $ec->on('process.error', function($actionName, $error) {
            if ($this->state->state() === State::RECOVERY) {
                $this->logger->critical("Action $actionName failed again during recovery", ['error' => $error]);
            }
        });

        $ec->once('process.error', function($actionName, $error) {
            //Only call shutdown once if we fail during recovery
            if ($this->state->state() === State::RECOVERY) {
                $this->loop->futureTick(function() {
                    $this->shutdown();
                });
            }
        });

        $ec->on('idle', function() {
            if ($this->state->isStopping() && $this->sourceExecutionCoordinator->getRunningProcessCount() === 0) {
                $this->logger->debug('All processes have stopped');
                /** If we are shutting down and all input and action processes have stopped then continue the shutdown process straight away instead of waiting for the timers */
                $this->exit();
            }
        });

        $ec->on('action.undefined', function($action) {
            //@Note: Error not resolvable in current runtime
            $this->logger->debug("Unknown action {$action->getCmd()}");
            $error = [
                'error' => [
                    'code' => -1,
                    'message' => "Action {$action->getCmd()} is undefined",
                ],
                'action' => $action,
            ];
            $this->erroredActionCommands[] = $error;
        });

        $ec->on('action.argumenterror', function($action, $exception) {
            //@Note: Error not resolvable in current runtime
            $error = [
                'error' => [
                    'code' => $exception->getCode(),
                    'message' => $exception->getMessage(),
                ],
                'action' => $action,
            ];
            $this->erroredActionCommands[] = $error;
        });
    }

    protected function initialiseSourceExecution() : void {
        $this->sourceExecutionCoordinator = $sec = new SourceExecutionCoordinator($this->loop);

        $sec->on('event', $this->handleEvent(...));

        $sec->on('dirty', function() {
            $this->dirty = true;
        });

        $sec->on('log', function($logLevel, $message, $context) {
            $this->logger->log($logLevel, $message, $context);
        });

        $sec->on('error', function($data) {
            if (isset($data['exception'])) {
                $this->logger->error("{id}", ['id' => $data['id'] ?? '', 'exception' => $data['exception']]);
            } elseif (isset($data['message'])) {
                $this->logger->critical($data['message'], $data);
            }
        });

        $sec->on('essential_exit', function($id) {
            $this->shutdown();
        });

        $sec->on('all_processes_stopped', function() {
            if (!$this->state->isStopping()) {
                $this->logger->info("No more input processes running. Shutting down");
                $this->shutdown();
            } else {
                $this->shutdown_phase2();
            }
        });
    }

    public function register_action(string $name, string|array|callable $cmd, ?string $wd = null, ?bool $singleShot = false, array $env = [], object|array|null $schema = null) : void
    {
        $this->actionExecutionCoordinator->register_action($name, $cmd, $wd, $singleShot, $env, $schema);
    }

    /**
     * @param string $filename
     */
    public function setSavefileName(string $filename) : void
    {
        $this->saveFileName = $filename;
    }

    protected function setup_save_state() : void
    {
        /**
         * Set up a time to save the state of the correlation engine every saveStateSeconds
         */
        $this->saveStateHandler->on('save:failed', function($exception) {
            $this->dirty = true;
            if (!$this->state->isStopping()) { //Failure is expected if the save handler is running when the scheduler starts shutting down. A sync save state will be run at end of shutdown
                $this->logger->critical("Save state async failed.", ['exception' => $exception]);
            }
        });

        $this->scheduledTasks['scheduledSaveState'] = $this->loop->addPeriodicTimer($this->saveStateSeconds, function() {
            static $skipCount = 0;
            if (($this->dirty || $this->engine->isDirty()) && false === $this->saveStateHandler->asyncSaveInProgress())
            {
                /** Clear the dirty flags before calling the async save process.
                 * This ensures that changes that occur between now and the save file
                 * being written are scheduled to be flushed on the next cycle
                 */
                $this->engine->clearDirtyFlag();
                $this->dirty = false;
                $skipCount = 0;
                $this->saveStateHandler->saveStateAsync($this->buildState());
            } else if ($this->saveStateHandler->asyncSaveInProgress()) {
                if ((++$skipCount % 5) === 0) {
                    $this->logger->warning("skipping save, another save is still in process (attempt: $skipCount)");
                }
            }
        });
        $this->logger->debug("Save handler configured for " . $this->saveStateSeconds . ' seconds');

        /** Set up an hourly time to save state (or skip if we are already saving state when this timer fires) */
        $this->scheduledTasks['hourlySaveState'] = $this->loop->addPeriodicTimer(3600, function() {
            if (false === $this->saveStateHandler->asyncSaveInProgress()) {
                $this->engine->clearDirtyFlag();
                $this->dirty = false;
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
     * @param int $seconds
     * @param int $initialDelay
     * @return void
     */
    public function setHeartbeatInterval(int $seconds, int $initialDelay = 0) : void {
        $this->heartbeat_seconds = $seconds;
        $this->heartbeat_initialDelay = $initialDelay;
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
            $this->panic($ex);
        }
        $restoring = $savedState !== false;

        if ($restoring) {
            try {
                $this->setState($savedState['scheduler']);
                $this->engine->setState($savedState['engine']);
            } catch (Throwable $ex) {
                $this->logger->emergency("A fatal exception was thrown while loading previous saved state.", ['exception' => $ex]);
                $this->panic($ex);
            }
            $this->logger->debug("Successfully loaded from saved state");
        }
        unset($savedState);

        /** Force a run of the PHP GC and release caches. This helps clear out memory consumed by restoring state from a large json file */
        $this->memoryReclaim();

        /** Inject some synthetic events in to the engine to flag that the engine is starting for the first time or restoring
         * Rules can handle these events for initialisation purposes (handy for setting up rules that detect when an event is missing)
         */
        $this->loop->futureTick(function() use ($restoring) {
            $event = $restoring ? new Event(['event' => static::CONTROL_MSG_RESTORED_STATE]) : new Event(['event' => static::CONTROL_MSG_NEW_STATE]);

            /**
             * Pass the event to the engine to be handled
             */
            $this->handleEvent($event);
        });
    }

    /**
     * Start the Engine
     */
    public function run() : void
    {
        /** Opcache is disabled if either the extension isn't loaded or opcache_get_status returns false */
        if (!(function_exists('opcache_get_status') && opcache_get_status(false) !== false)) {
            $this->logger->warning("*** Opcache is not enabled. This will reduce performance and increase memory usage ***");
        }

        if (!gc_enabled()) {
            gc_enable();
            $this->logger->info("Garbage collection enabled at runtime");
        }

        if ($this->logger instanceof NullLogger) {
            error_log('Warning: Logger is set to NullLogger, not further logs will be seen');
        }
        $this->logger->debug("Using event loop implementation: {class}", ['class' => get_class($this->loop)]);

        /** Initialise the management server early in the startup */
        $this->initialiseManagementServer();

        /** Restore the state of the scheduler and engine */
        $this->saveStateHandler = new FileAdapter($this->saveFileName, $this->logger, $this->loop);
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
                    $this->handleEvent($event);
                });
            } else {
                $action = new Action($this->newEventAction, $event);
                $this->engine->emit('action', [$action]);
            }
        });

        /** Handle request to run an action */
        $this->engine->on('action', $this->handleAction(...));

        /** Handle request to run an on demand source */
        $this->engine->on('source', function(Scheduler\Messages\ExecuteSource $execute) {
            $config = $this->sourceExecutionCoordinator->getInputProcessesConfig();
            if (isset($config[$execute->getCmd()])) {
                $cfg = $config[$execute->getCmd()];
                $rndid = $execute->getCmd() . '_' . bin2hex(random_bytes(4)); //Generate unique ID for the on demand run
                $env = ($cfg['env'] ?? []) + $execute->getVars();
                //Register the on demand source
                $this->register_input_process($rndid, $cfg['cmd'], $cfg['wd'], $env, false, false);
                $process = $this->sourceExecutionCoordinator->initialise_input_process($rndid);
                $process->on('exit', function() use ($rndid) {
                    $this->unregister_input_process($rndid); //Remove config once process exits
                });
            } else {
                $this->logger->error("Unable to start unknown on demand source " . json_encode($execute->getCmd()));
            }
        });

        /** If we have any errored actions then we replay them and attempt recovery. In normal state we initialise the input processes */
        $erroredActions = $this->erroredActionCommands;
        if ($erroredActions) {
            //Function based actions will run straight away and may error again (@TODO: Can we delay function based actions)
            $this->erroredActionCommands = [];
            $this->logger->notice('Beginning failed action recovery process');
            $this->state->transition(State::RECOVERY);
            $retryable = [];
            foreach($erroredActions as $errored) {
                if (is_array($errored['error'])) {
                    $code = $errored['error']['code'] ?? 0;
                    if ($code < 0 && $code !== Error::INTERNAL_ERROR) {
                        $this->logger?->debug("Action request with non-retryable error code", ["code" => $code, "cmd" => $errored['action']['cmd'], "params" => $errored['action']['vars']]);
                        continue;
                    }
                }
                $retryable[] = new Action($errored['action']['cmd'], $errored['action']['vars']);
            }
            if ($retryable) {
                $this->actionExecutionCoordinator->once('idle', function () {
                    if ($this->state->state() === State::RECOVERY && count($this->erroredActionCommands) === 0) {
                        $this->logger->info('Replay of errored actions completed successfully. Resuming normal operations');
                        $this->state->transition(State::STARTING);
                        $this->saveStateHandler->saveStateSync($this->buildState());
                        $this->sourceExecutionCoordinator->initialise_input_processes();
                        $this->state->transition(State::RUNNING);
                    }
                });
                foreach($retryable as $action) {
                    $this->engine->emit('action', [$action]);
                }
            } else {
                //No actions were retryable so we continue with booting the Scheduler
                $this->state->transition(State::STARTING);
                $this->saveStateHandler->saveStateSync($this->buildState());
                $this->sourceExecutionCoordinator->initialise_input_processes();
                $this->state->transition(State::RUNNING);
            }
        } else {
            $this->sourceExecutionCoordinator->initialise_input_processes();
            $this->state->transition(State::RUNNING);
        }

        /** Initialise the state saving task */
        $this->setup_save_state();

        /** Initialise Heartbeat timer */
        if ($this->heartbeat_seconds > 0) {
            $heartbeat = new Heartbeat($this->heartbeat_seconds);

            $heartbeat->on('pulse', function($runtime, $seq) {
                $this->handleEvent(new Event(['event' => static::CONTROL_MSG_HEARTBEAT, 'runtime' => $runtime, 'seq' => $seq]));
            });

            $heartbeat->start($this->loop, $this->heartbeat_initialDelay);
        }

        $this->loop->addPeriodicTimer(5, function() {
            // Update metric for current memory usage (percentage)
            $this->metricsCollector->set('memory', 'percent_used', $this->currentMemoryPercentUsed);
            $this->metricsCollector->set('memory', 'total_used', memory_get_usage(true));

            // Also expose the number of inflight actions
            $this->metricsCollector->set('actions', 'inflight', $this->actionExecutionCoordinator->inflightActionCount());
        });

        //Log stats coming from the Correlation Engine
        $this->engine->on('stat', $this->metricsCollector->set(...));

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
            $this->memoryReclaim();
            $this->saveStateHandler->saveStateSync($this->buildState());
        });

        /** GO! */
        $this->loop->run();
        if ($this->state->state() === State::RUNNING) {
            $this->state->transition(State::STOPPED_UNCLEAN);
        }
    }

    /**
     * Initialise shutdown by stopping processes and timers
     */
    public function shutdown() : void {
        $this->state->transition(State::STOPPING);
        $this->sourceExecutionCoordinator->setStopping(true);
        while ($task = array_pop($this->scheduledTasks)) {
            $this->loop->cancelTimer($task);
            $task = null;
        }

        if ($this->sourceExecutionCoordinator->getRunningProcessCount() > 0) {
            $this->sourceExecutionCoordinator->shutdown();

            //Set up a timer to forcefully move the scheduler into the next shutdown phase if the input's have not shutdown in time
            $this->shutdownTimer = $this->loop->addTimer(10.0, function() {
                $this->logger->warning( "Input processes did not shutdown within the timeout delay...");
                $this->shutdown_phase2();
            });
        } else {
            $this->shutdown_phase2();
        }
    }

    /**
     * Input processes are stopped. Shutdown any running actions (some may need to be flushed)
     */
    protected function shutdown_phase2() : void
    {
        $this->state->transition(State::STOPPING);
        if ($this->shutdownTimer) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }
        /**
         * Notify any rules listening for a Stop event that we are stopping
         */
        $this->logger->debug( "Notify CorrelationEngine that we are stopping");
        $this->handleEvent(new Event(['event' => static::CONTROL_MSG_STOP]));

        /** Check if we have any running action commands, if we do then some actions may not have completed and/or need to flush+complete tasks. Send them a SIGTERM to complete their shutdown */
        if ($this->actionExecutionCoordinator->isIdle()) {
            $this->exit();
        } else {
            $this->logger->debug("Shutting down running action processes");
            $this->actionExecutionCoordinator->shutdown();

            $this->loop->futureTick(function() {
                //Set up a timer to forcefully stop the scheduler if all processes don't terminate in time
                $this->shutdownTimer = $this->loop->addTimer(20.0, function () {
                    //if (!$this->actionExecutionCoordinator->isIdle()) {
                        $this->logger->warning("Action processes did not shutdown within the timeout delay...");
                    //}
                    $this->exit();
                });
            });
        }
    }

    /**
     * Stop the Loop and sync state to disk
     */
    public function exit() : void {
        if ($this->shutdownTimer) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }
        $this->loop->futureTick(function() {
            $this->loop->stop();
            if ($this->actionExecutionCoordinator->isIdle()) {
                $this->state->transition(State::STOPPED);
            } else {
                $this->logger->error("There were still inflight action commands at shutdown.", [
                    'inflight' => $this->actionExecutionCoordinator->getInflightActionCommands(),
                    'closures' => $this->actionExecutionCoordinator->getInflightActionClosures(),
                ]);
                $this->state->transition(State::STOPPED_UNCLEAN);
            }
            $this->logger->debug("Event Loop stopped");
            if (isset($this->saveStateHandler)) {
                $this->saveStateHandler->saveStateSync($this->buildState()); //Loop is stopped. Do a blocking synchronous save of current state prior to exit.
                unset($this->saveStateHandler);
            }
        });
    }

    /** Call if we fail in a non-handled way
     * @throws Throwable
     */
    protected function panic(Throwable $t) : void {
        Loop::stop();
        throw $t;
    }

    /**
     * @return array
     */
    protected function getState() : array
    {
        $state = [];
        $state['state'] = $this->state->state();
        $state['uptime_msec'] = (int)round((hrtime(true) - $this->schedulerStartTime)/1e+6);
        $state['input']['running'] = array_keys($this->sourceExecutionCoordinator->getInputProcesses());
        $state['input']['checkpoints'] = $this->sourceExecutionCoordinator->getInputProcessesCheckpoints();
        $state['input']['rpc_packet_sizes'] = array_map(fn($s) => [
            $s->getHistogram(),
        ], $this->sourceExecutionCoordinator->getInputRpcPacketSizes());
        $state['actions'] = [
            'inflight' => array_map(function($action) { return $action['action']; }, $this->actionExecutionCoordinator->getInflightActionCommands()),
            'errored' => $this->erroredActionCommands,
            'rpc_packet_sizes' => $this->actionExecutionCoordinator->getRpcPacketSizes(),
        ];
        $state['inputPaused'] = $this->pausedOnMemoryPressure ? 'Yes' : 'No';
        $state['pausedCount'] = $this->pausedOnMemoryPressureCount;
        $state['memoryPercentageUsed'] = $this->currentMemoryPercentUsed;
        $state['saveFileSizeBytes'] = $this->saveStateHandler?->lastSaveSizeBytes();
        $state['saveStateLastDuration'] = $this->saveStateHandler?->lastSaveWriteDuration();
        //@TODO add a clean shutdown flag here in save state
        return $state;
    }

    /**
     * @param array $state
     */
    protected function setState(array $state) : void
    {
        $this->sourceExecutionCoordinator->setInputProcessesCheckpoints($state['input']['checkpoints']);
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
        /** @TODO: Fix up */
        if (true === static::PRESERVE_FAILED_EVENTS_ONLOAD) {
            $this->erroredActionCommands = $state['actions']['errored'];
            unset($state['actions']['errored']);
            while(count($state['actions']['inflight'])) {
                $inflight = array_shift($state['actions']['inflight']);
                $this->erroredActionCommands[] = [
                    'error' => [
                        'code' => 2,
                        'message' => 'Inflight when process exited',
                    ],
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

        $percent_used = (int)round(($current_memory_usage / $this->memoryLimit) * 100);

        /** Try releasing memory first and recalculate percentage used */
        if ($percent_used >= static::MEMORY_PRESSURE_HIGH_WATERMARK) {
            /** Running this every check cycle negatively impacts the scheduler's performance,
             *   however, since we are paused (or going to pause) at this stage, and are awaiting the external action processes to complete the actual impact will be minimal
             */
            $this->memoryReclaim();
            $current_memory_usage = memory_get_usage();
            $percent_used = (int)round(($current_memory_usage / $this->memoryLimit) * 100);
        }

        $this->currentMemoryPercentUsed = $percent_used;

        if (false === $this->pausedOnMemoryPressure &&
                ($percent_used >= static::MEMORY_PRESSURE_HIGH_WATERMARK ||
                    $this->actionExecutionCoordinator->inflightActionCount() > static::RUNNING_ACTION_LIMIT_HIGH_WATERMARK)
        )
        {
            $this->logger->warning(
                "Currently using $percent_used% of memory limit with {$this->actionExecutionCoordinator->inflightActionCount()} inflight actions. Pausing input processes");

            $this->sourceExecutionCoordinator->pauseAllProcesses();
            $this->pausedOnMemoryPressure = true;
            ++$this->pausedOnMemoryPressureCount;

            $inflightActionCount = $this->actionExecutionCoordinator->inflightActionCount();
            /** @TODO take into account delaying shutdown if we still have some outstanding actions and memory usage is dropping */
            $this->scheduledTasks['pausedOnMemoryPressureTimer'] = $this->loop->addPeriodicTimer(300, function() use (&$inflightActionCount) {
                $currentActionCount = $this->actionExecutionCoordinator->inflightActionCount();
                if ($currentActionCount < $inflightActionCount) {
                    $this->logger->debug("Current action count dropped from {old} to {new}", ['old' => $inflightActionCount, 'new' => $currentActionCount]);;
                }
                if ($this->pausedOnMemoryPressure && $currentActionCount >= $inflightActionCount) {
                    $this->logger->critical("Timeout! Input processes are still paused and inflight actions not reducing, shutting down");
                    $this->shutdown();
                }
                $inflightActionCount = $currentActionCount;
            });
        }
        else
        {
            if ($this->pausedOnMemoryPressure &&
                $percent_used <= static::MEMORY_PRESSURE_LOW_WATERMARK &&
                $this->actionExecutionCoordinator->inflightActionCount() < static::RUNNING_ACTION_LIMIT_LOW_WATERMARK) {

                //Cancel the memory pressure timeout
                if ($this->scheduledTasks['pausedOnMemoryPressureTimer'] !== null) {
                    $this->loop->cancelTimer($this->scheduledTasks['pausedOnMemoryPressureTimer']);
                    unset($this->scheduledTasks['pausedOnMemoryPressureTimer']);
                }
                $this->memoryReclaim();
                //Resume input
                $this->sourceExecutionCoordinator->resumeAllProcesses();
                $this->pausedOnMemoryPressure = false;
            }
        }
    }

    /**
     * Run memory reclaim
     */
    protected function memoryReclaim() : void {
        $mark = hrtime(true);
        $memCurr = memory_get_usage();
        $cycles = gc_collect_cycles();
        $bytes = gc_mem_caches();
        $saved = max(0, $memCurr - memory_get_usage()); //Don't show a negative value if we don't release anything
        $time = (int)round((hrtime(true)-$mark)/1e+3);
        $this->logger->debug("GC Run Complete in $time μs {cycles: $cycles, reclaim: $bytes, reduced: $saved bytes, current: " . round(memory_get_usage() / 1048576,2) . "MB, max: " . round(memory_get_peak_usage() / 1048576,2) ."MB}");
        if (function_exists('memory_reset_peak_usage')) {
            \memory_reset_peak_usage();
        }
    }

    /**
     * Enable or disable the experimental management server
     * @param bool $enable
     */
    public function enableManagementServer(bool $enable): void
    {
        $this->enabledManagementServer = $enable;
    }

    /**
     * Initialise the management server if enabled
     */
    protected function initialiseManagementServer(): void
    {
        if ($this->enabledManagementServer) {
            $this->managementServer = new Server($this);
            $this->logger->info("Management server listening on " . ($this->managementServer->getListeningAddress() ?? 'Unable to retrieve address'));
        }
    }

    /**
     * @param int $interval
     * @return void
     * Enable logging of metrics to a PSR-3 logger
     */
    public function scheduleStatisticsLogging(int $interval): void
    {
        // Log metrics
        $this->metricsCollector->schedulePeriodicLogging($this->loop, $interval);
    }

    public function getConfig() : array {
        return [
            'rules' => $this->rules,
            'input' => $this->sourceExecutionCoordinator->getInputProcessesConfig(),
            'actions' => $this->actionExecutionCoordinator->getConfig(),
        ];
    }
}
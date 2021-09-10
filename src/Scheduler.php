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
use function fwrite;
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

use const STDERR;
use const PHP_EOL;
use const SIGINT;
use const SIGTERM;
use const SIGHUP;

/**
 * Class Scheduler
 * @package EdgeTelemetrics\EventCorrelation
 */
class Scheduler {
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

    /** @var array Accepted input RPC methods */
    const INPUT_ACTIONS = [ self::INPUT_ACTION_HANDLE, self::INPUT_ACTION_CHECKPOINT ];

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

    public function __construct(array $rules)
    {
        $this->rules = $rules;
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
            fwrite(STDERR, "Failed to start input process $id, RuntimeException: " . $exception->getMessage() . PHP_EOL);
            return;
        }
        $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

        /**
         * Handle RPC call from the input process
         */
        $process_decoded_stdout->on('data', function(JsonRpcNotification $rpc) use ($id) {
            switch ( $rpc->getMethod() ) {
                case self::INPUT_ACTION_HANDLE:
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
                default:
                    throw new RuntimeException("Unknown json rpc command {$rpc->getMethod()} from input process");
            }
        });

        /**
         * Log any errors we receive on the processed STDERR, error is a fatal event and the stream will be closed, so we need to terminate the process since it can no longer communicate with us
         */
        $process_decoded_stdout->on('error', function(Exception $error) use ($id, $process) {
            fwrite(STDERR, "$id error: " . $error->getMessage() . PHP_EOL);
            $process->terminate(SIGTERM);
        });

        /**
         * Input processes STDOUT has closed, if we are not in the process of shutting down then we need to terminate the process since it can no longer communicate with us
         */
        $process->stdout->on('close', function() use ($id, $process) {
            if (!$this->shuttingDown && $process->isRunning()) {
                fwrite(STDERR, "$id STDOUT closed unexpectedly, terminating process" . PHP_EOL);
                $process->terminate(SIGTERM);
            }
        });

        /**
         * Log STDERR messages from input processes
         */
        $process->stderr->on('data', function($data) use ($id) {
            fwrite(STDERR, "$id message: " . trim($data) . PHP_EOL);
        });

        /**
         * Handle process exiting
         */
        $process->on('exit', function($code, $term) use($id) {
            /** Remove process from table  */
            unset($this->input_processes[$id]);

            if ($term === null) {
                fwrite(STDERR, "Input Process $id exited with code: $code" . PHP_EOL);
            } else {
                fwrite(STDERR, "Input Process $id exited on signal: $term" . PHP_EOL);
            }
            if ($code === 255) { //255 = PHP Fatal exit code
                fwrite(STDERR, "Input process $id exit was due to fatal PHP error" . PHP_EOL);
            }
            if ($code !== 0 && false === $this->shuttingDown) {
                /**
                 * @TODO Implement restart of input processes if process terminated with an error and we are not shutting down ($this->shuttingDown)
                 */
            }
            /** We stop processing if there are no input processes available **/
            if (0 === count($this->input_processes)) {
                fwrite(STDERR, "No more input processes running. Shutting down" . PHP_EOL);
                $this->stop();
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
            /** If there is no running action then we initialise the process */
            $process = new Process($actionConfig['cmd'], $actionConfig['wd'], $actionConfig['env']);
            $process->start($this->loop);

            $process->stderr->on('data', function ($data) {
                //@TODO handle any error messages
                fwrite(STDERR, $data . "\n");
            });

            $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

            /** Handler for the Json RPC response */
            $process_decoded_stdout->on('data', function (JsonRpcResponse $response) {
                if ($response->isSuccess()) {
                    /** Once the action has been processed successfully we can discard of our copy of it */
                    unset($this->inflightActionCommands[$response->getId()]);
                } else {
                    /** Transfer the action from the running queue to the errored queue
                     * @TODO We need to watch this queue and handle any run-away errors (eg a database been unavailable to ingest events)
                     * @TODO This should be put into a function as we call it both here and when an action terminates unexpectedly
                     */
                    $error = [
                        'error' => $response->getError(),
                        'action' => $this->inflightActionCommands[$response->getId()]['action'],
                    ];
                    $this->erroredActionCommands[] = $error;
                    unset($this->inflightActionCommands[$response->getId()]);

                    /** @TODO We should be placing these errors into an external system to be logged and possibly processed */
                    fwrite(STDERR, $response->getError()->getMessage() . " : " . json_encode($response->getError()->getData()) . PHP_EOL);
                }
                /** Release memory used by the inflight action table */
                if (count($this->inflightActionCommands) === 0)
                {
                    $this->inflightActionCommands = [];
                }
                $this->dirty = true;
            });

            $process->on('exit', function ($code, $term) use ($actionName, $process) {
                /** Action has terminated. If it successfully completed then it will have sent an ack on stdout first before exit */
                if ($term === null) {
                    fwrite(STDERR, "Action $actionName exited with code: $code" . PHP_EOL);
                } else {
                    fwrite(STDERR, "Action $actionName exited on signal: $term" . PHP_EOL);
                }
                if ($code === 255) { //255 = PHP Fatal exit code
                    fwrite(STDERR, "Action $actionName exit was due to fatal PHP error" . PHP_EOL);
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
            fwrite(STDERR, "Error creating temporary save state file\n");
            return;
        }
        $file = $filesystem->file($filename);
        $file->putContents(json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES))->then(function () use ($file) {
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
            fwrite(STDERR, "Save state async failed. " . $ex->getMessage() . "\n");
        });
    }

    /**
     * Save the current state in a synchronous manner
     */
    public function saveStateSync()
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        if (false === $filename) {
            fwrite(STDERR, "Error creating temporary save state file\n");
            return;
        }
        file_put_contents($filename, json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        rename($filename, $this->saveFileName);
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

        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);

        if ($restoring) {
            try {
                $this->setState($savedState['scheduler']);
                $this->engine->setState($savedState['engine']);
            } catch (Exception $ex) {
                fwrite(STDERR, "A fatal exception was thrown while loading previous saved state. " . $ex->getMessage() . PHP_EOL);
                exit(-1);
            }
            fwrite(STDERR, "Successfully loaded from saved state" . PHP_EOL);
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
            fwrite(STDERR, "Opcache is not enabled. This will reduce performance and increase memory usage" . PHP_EOL);
        }

        $this->loop = Loop::get();
        fwrite(STDERR, "Using event loop implementation: " . get_class($this->loop) . PHP_EOL);

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
            $process = new Process('exec ' . $config['cmd'], $config['wd'], $env);
            $this->setup_input_process($process, $id);
            if ($process->isRunning()) {
                fwrite(STDERR, "Started input process $id" . PHP_EOL);
                $this->input_processes[$id] = $process;
            } else {
                fwrite(STDERR, "An input process failed to start, exiting" . PHP_EOL);
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
                $this->engine->handle($event);
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
                echo "Unknown Action: " . json_encode($action) . PHP_EOL;
            }
        });

        /** Initialise the state saving task */
        $this->setup_save_state();

        /** Monitor memory usage */
        $this->memoryLimit = $this->calculateMemoryLimit();
        fwrite(STDERR, "Memory limit set to $this->memoryLimit Bytes" . PHP_EOL);
        $this->loop->addPeriodicTimer(2, function() { $this->checkMemoryPressure(); });

        /** Gracefully shutdown */
        // ctrl+c
        $this->loop->addSignal(SIGINT, function() {
            fwrite(STDERR, "received SIGINT scheduling shutdown...\n");
            $this->shutdown();
        });
        // kill
        $this->loop->addSignal(SIGTERM, function() {
            fwrite(STDERR, "received SIGTERM scheduling shutdown...\n");
            $this->shutdown();
        });
        // logout
        $this->loop->addSignal(SIGHUP, function() {
            /** If we receive a HUP save the current running state. Don't exit
             * Force a run of the PHP GC and release caches.
             */
            fwrite(STDERR, "SIGHUP received, clearing caches and saving non-dirty state\n");
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
        }

        if (count($this->input_processes) > 0) {
            fwrite(STDERR, "Shutting down running input processes\n");
            foreach ($this->input_processes as $processKey => $process) {
                if (false === $process->terminate(SIGTERM)) {
                    fwrite(STDERR, "Unable to send SIGTERM to input process $processKey\n");
                }
                if ($process->isStopped()) {
                    $process->terminate(SIGCONT); // If our input processes are paused by memory pressure then we need to send SIGCONT after SIGTERM as they are currently stopped
                }
            }
        }
        //Set up a timer to forcefully stop the scheduler if all processes don't terminate in time
        $this->loop->addTimer(10.0, function() {
            fwrite(STDERR, "shutdown timeout...\n");
            $this->stop();
        });
    }

    /**
     * Stop the Loop and sync state to disk
     */
    public function stop()
    {
        if (count($this->inflightActionCommands)) {
            fwrite(STDERR, "Error: There were running action commands at the time of stopping\n");
        }
        $this->loop->stop();
        fwrite(STDERR, "Event Loop stopped\n");
        $this->saveStateSync(); //Loop is stopped. Do a blocking synchronous save of current state prior to exit.
    }

    /**
     * @return false|mixed
     */
    public function loadStateFromFile()
    {
        if (file_exists($this->saveFileName))
        {
            return json_decode(file_get_contents($this->saveFileName), true);
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
            fwrite(STDERR, "Failed actions detected from previous execution");
            if (count($state['actions']['errored']) > 50)
            {
                fwrite(STDERR,"Large number of failed actions. Memory consumption for state table may be large.");
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
            fwrite(STDERR,
                "Currently using $percent_used% of memory limit with " . count($this->inflightActionCommands) . " inflight actions. Pausing input processes" . PHP_EOL);

            foreach($this->input_processes as $processId => $process)
            {
                if ($process->isRunning()) {
                    $process->terminate(SIGSTOP);
                    fwrite(STDERR, "Paused input process $processId" . PHP_EOL);
                }
            }
            $this->pausedOnMemoryPressure = true;
            ++$this->pausedOnMemoryPressureCount;

            /** @TODO take into account delaying shutdown if we still have some outstanding actions and memory usage is dropping */
            $this->pausedOnMemoryPressureTimeout = $this->loop->addTimer(300, function() {
                if ($this->pausedOnMemoryPressure) {
                    fwrite(STDERR, "Timeout! Input processes are still paused, shutting down\n");
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
                    fwrite(STDERR, "Resuming input process $processId" . PHP_EOL);
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
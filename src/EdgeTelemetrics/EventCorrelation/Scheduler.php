<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use React\EventLoop\Factory;
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
use function memory_get_peak_usage;
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
use function error_log;
use function ini_get;
use function preg_match;
use function strtoupper;

use const STDERR;
use const PHP_EOL;
use const SIGINT;
use const SIGTERM;
use const SIGHUP;

class Scheduler {
    const CHECKPOINT_VARNAME = 'PHPEC_CHECKPOINT';

    /**
     * @var LoopInterface
     */
    protected $loop;

    /**
     * @var CorrelationEngine
     */
    protected $engine;

    /**
     * @var TimerInterface
     */
    protected $nextTimer;

    /**
     * @var array React\ChildProcess
     */
    protected $input_processes = [];

    /**
     * @var array Class list of rules
     */
    protected $rules = [];

    /**
     * @var array Process Configuration for input processes
     */
    protected $input_processes_config = [];

    /**
     * @var array
     */
    protected $input_processes_checkpoints = [];

    /**
     * @var array React\ChildProcess Process table to keep track of all action processess that are running.
     */
    protected $runningActions = [];

    /**
     * @var array Configuration for actions
     */
    protected $actionConfig = [];

    /**
     * @var array
     */
    protected $inflightActionCommands = [];

    /**
     * @var array
     */
    protected $erroredActionCommands = [];

    /**
     * @var TimerInterface Reference to the periodic task to save state.
     */
    protected $saveHandler;

    protected $saveFileName = '/tmp/php-ec-savepoint';

    /**
     * @var bool Flag if the scheduler has information that needs to be flushed to the save file.
     */
    protected $dirty;

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
    protected $newEventAction = null;

    /** @var string RPC Method expected to handle an event */
    const INPUT_ACTION_HANDLE = 'handle';

    /** @var string RPC Method to record a checkpoint for an import process */
    const INPUT_ACTION_CHECKPOINT = 'checkpoint';

    /** @var array Accepted input RPC methods */
    const INPUT_ACTIONS = [ self::INPUT_ACTION_HANDLE, self::INPUT_ACTION_CHECKPOINT ];

    /** @var string RPC method name to get an action handler to run a request */
    const ACTION_RUN_METHOD = 'run';

    public function __construct(array $rules)
    {
        $this->rules = $rules;
    }

    public function register_input_process($id, string $cmd, ?string $wd = null, array $env = [])
    {
        $this->input_processes_config[$id] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env];
    }

    public function setup_input_process(Process $process, $id) {
        $process->start($this->loop);
        $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

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

        $process->stderr->on('data', function($data) {
            fwrite(STDERR, $data . "\n");
        });

        $process->on('exit', function($code, $term) use($process) {
            if ($term === null) {
                fwrite(STDERR, "Input Process {$process->getCommand()} exited with code: $code" . PHP_EOL);
            } else {
                fwrite(STDERR, "Input Process {$process->getCommand()} exited on signal: $term" . PHP_EOL);
            }
        });
    }

    public function setNewEventAction($actionName)
    {
        $this->newEventAction = $actionName;
    }

    public function register_action(string $name, string $cmd, ?string $wd = null, ?bool $singleShot = false, array $env = [])
    {
        $this->actionConfig[$name] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env, 'singleShot' => $singleShot];
    }

    public function start_action($actionName)
    {
        $actionConfig = $this->actionConfig[$actionName];
        /** Handle singleShot processes true === $actionConfig['singleShot'] ||  */
        if (!isset($this->runningActions[$actionName])) {
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
                     */
                    $error = [
                        'error' => $response->getError(),
                        'action' => $this->inflightActionCommands[$response->getId()],
                    ];
                    $this->erroredActionCommands[] = $error;
                    unset($this->inflightActionCommands[$response->getId()]);

                    fwrite(STDERR, $response->getError()->getMessage() . PHP_EOL);
                }
                /** Release memory used by the inflight action table */
                if (count($this->inflightActionCommands) === 0)
                {
                    $this->inflightActionCommands = [];
                }
                $this->dirty = true;
            });

            $process->on('exit', function ($code, $term) use ($actionName) {
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
                    /** @TODO Go through inflight actions and look for any that match our exiting with error action. Mark them as errored, otherwise they stay in the inflight action commands queue */
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

    public function setSavefileName(string $filename)
    {
        $this->saveFileName = $filename;
    }

    public function setup_save_state()
    {
        /**
         * Setup a time to save the state of the correlation engine every every one second
         */
        $filesystem = Filesystem::create($this->loop);
        $this->saveHandler = $this->loop->addPeriodicTimer(1, function() use ($filesystem) {
            if ($this->engine->isDirty() || $this->dirty)
            {
                /** Clear the dirty flags before calling the async save process.
                 * This ensures that changes that occur between now and the save file
                 * being written are scheduled to be flushed on the next cycle
                 */
                $this->engine->clearDirtyFlag();
                $this->dirty = false;
                $this->saveStateAsync($filesystem);
            }
        });
    }

    public function buildState()
    {
        return ['engine' => $this->engine->getState(),
            'scheduler' => $this->getState(),
        ];
    }

    public function saveStateAsync(FilesystemInterface $filesystem)
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        $file = $filesystem->file($filename);
        $file->putContents(json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_NUMERIC_CHECK))
            ->then(function () use ($file) {
                $file->rename($this->saveFileName)
                    ->then(function (\React\Filesystem\Node\FileInterface $newfile) {
                        //Everything Good
                    }, function (\Exception $e) {
                        $this->dirty = true; /** We didn't save state correctly so we mark the scheduler as dirty to ensure it is attempted again */
                        throw $e;
                    });
            }, function (\Exception $e) {
                throw $e;
            });
    }

    public function saveStateSync()
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        file_put_contents($filename, json_encode($this->buildState(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_NUMERIC_CHECK));
        rename($filename, $this->saveFileName);
    }

    /**
     * @throws \Exception
     */
    function scheduleNextTimeout()
    {
        /**
         * Cancel current timeout and setup the next
         */
        if (null !== $this->nextTimer) {
            $this->loop->cancelTimer($this->nextTimer);
        }

        $timeouts = $this->engine->getTimeouts();
        if (!empty($timeouts)) {
            $nextTimeout = $timeouts[array_key_first($timeouts)];
            $now = new \DateTimeImmutable();
            $difference = $nextTimeout['timeout']->getTimestamp() - $now->getTimestamp();

            echo "Next timeout : {$nextTimeout['timeout']->format("c")}, Now: {$now->format("c")} \n";

            //If timeout has already past then manually check, this may block if we have fallen behind
            if ($difference <= 0) {
                echo "Timeout already passed\n";
                $this->engine->checkTimeouts(new \DateTimeImmutable());
                $this->scheduleNextTimeout();
            } else {
                $this->nextTimer = $this->loop->addTimer($difference, function (){
                    /** We only set a timer for the next one required, however we many have a few to process at the
                     * same time. Use the check timeouts function to process any and all timeouts.
                     */
                    echo "Timeout passed\n";
                    $this->engine->checkTimeouts(new \DateTimeImmutable());
                    $this->scheduleNextTimeout();
                });
                echo "Timer set for {$this->nextTimer->getInterval()} seconds\n";
            }
        }
    }

    protected function restoreState()
    {
        /** Load State from save file */
        $savedState = $this->loadStateFromFile();
        if (false !== $savedState) {
            $this->setState($savedState['scheduler']);
            unset($savedState['scheduler']);
        }

        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);
        if (false !== $savedState) {
            $this->engine->setState($savedState['engine']);
            unset($savedState['engine']);
        }

        /** Inject some synthetic events in to the engine to flag that the engine is starting for the first time or restoring
         * Rules can handle these events for initialisation purposes (handy for setting up rules that detect when an event is missing)
         */
        $this->loop->futureTick(function() use ($savedState) {
            if (false === $savedState) {
                $event = new Event(['event' => self::CONTROL_MSG_NEW_STATE]);
            } else {
                $event = new Event(['event' => self::CONTROL_MSG_RESTORED_STATE]);
            }
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

    public function run()
    {
        if (!$this->isOpcacheEnabled())
        {
            fwrite(STDERR, "Opcache is not enabled. This will reduce performance and increase memory usage" . PHP_EOL);
        }

        $this->loop = Factory::create();
        fwrite(STDERR, "Using event loop implementation: " . get_class($this->loop) . PHP_EOL);

        /** Restore the state of the scheduler and engine */
        $this->restoreState();

        /** Force a run of the PHP GC and release caches. This helps clearing out memory consumed by restoring state from a large json file */
        gc_collect_cycles();
        gc_mem_caches();

        /** Start input processes */
        foreach ($this->input_processes_config as $id => $config)
        {
            $env = $config['env'];
            /** If we have a checkpoint in our save state pass this along to the input process via the ENV */
            if (isset($this->input_processes_checkpoints[$id]))
            {
                $env = array_merge([self::CHECKPOINT_VARNAME => json_encode($this->input_processes_checkpoints[$id])], $env);
            }
            $process = new Process($config['cmd'], $config['wd'], $env);
            $this->setup_input_process($process, $id);
            $this->input_processes[] = $process;
        }

        /** When new events are emitted from the correlation engine we persist them (hopefully) by sending them through an action */
        $this->engine->on('event', function(IEvent $event) {
            if (null !== $this->newEventAction) {
                $action = new Action($this->newEventAction, json_decode(json_encode($event), true));
                $this->engine->emit('action', [$action]);
            };
        });

        $this->engine->on('action', function(Action $action) {
            $actionName = $action->getCmd();
            if (isset($this->actionConfig[$actionName]))
            {
                $process = $this->start_action($actionName);
                /** Once the process is up and running we then write out our data via it's STDIN, encoded as a JSON RPC call */
                /** @TODO id should be a sequential number and not generated from mt_rand() */
                $rpc_request = new JsonRpcRequest(self::ACTION_RUN_METHOD, $action->getVars(), mt_rand());
                $this->inflightActionCommands[$rpc_request->getId()] = $action;
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
        $this->loop->addPeriodicTimer(2, function() { $this->checkMemoryPressure(); });

        /** Gracefully shutdown */
        // ctrl+c
        $this->loop->addSignal(SIGINT, array($this, 'stop'));
        // kill
        $this->loop->addSignal(SIGTERM, array($this, 'stop'));
        // logout
        $this->loop->addSignal(SIGHUP, function() {
            /** If we receive a HUP save the current running state. Don't exit
             * Force a run of the PHP GC and release caches.
             */
            gc_collect_cycles();
            gc_mem_caches();
            $this->saveStateSync();
        });

        /** GO! */
        $this->loop->run();
    }

    public function stop()
    {
        $this->loop->stop();
        $this->saveStateSync(); //Loop is stopped. Do a blocking synchronous save of current state prior to exit.
    }

    public function loadStateFromFile()
    {
        if (file_exists($this->saveFileName))
        {
            return json_decode(file_get_contents($this->saveFileName), true);
        }
        return false;

    }

    protected function getState()
    {
        $state = [];
        $state['input']['checkpoints'] = $this->input_processes_checkpoints;
        $state['actions'] = ['inflight' => $this->inflightActionCommands, 'errored' => $this->erroredActionCommands];
        return $state;
    }

    public function setState(array $state)
    {
        $this->input_processes_checkpoints = $state['input']['checkpoints'];
        /** If we had any actions still processing when we last saved state then move those to errored as we don't know if they completed */
        /** @TODO, this could be a big array, we need to handle that in a memory sensitive way */
        if (count($state['actions']['errored']) > 0)
        {
            error_log("Failed actions detected from previous execution");
            if (count($state['actions']['errored']) > 50)
            {
                error_log("Large number of failed actions. Memory consumption for state table may be large.");
            }
        }
        if (true === self::PRESERVE_FAILED_EVENTS_ONLOAD) {
            $this->erroredActionCommands = array_merge($state['actions']['inflight'], $state['actions']['errored']);
        }
    }

    protected function checkMemoryPressure()
    {
        static $limit = 0;
        static $paused = false;
        if ($limit === 0) {
            $limit = $this->calculateMemoryLimit();
        }

        $current_memory_usage = memory_get_usage();
        $peak_memory_usage = memory_get_peak_usage(true);

        if ($limit === -1) {
            return;
        }
        else {
            $percent_used = (int)(($current_memory_usage / $limit) * 100);

            /** Try releasing memory first and recalculate percentage used */
            if ($percent_used >= self::MEMORY_PRESSURE_HIGH_WATERMARK) {
                gc_collect_cycles();
                gc_mem_caches();
                $current_memory_usage = memory_get_usage();
                $percent_used = (int)(($current_memory_usage / $limit) * 100);
            }

            if ($percent_used >= self::MEMORY_PRESSURE_HIGH_WATERMARK ||
                count($this->inflightActionCommands) > self::RUNNING_ACTION_LIMIT_HIGH_WATERMARK)
            {
                fwrite(STDERR, "Currently using $percent_used% of memory limit with " . count($this->inflightActionCommands) . " inflight actions. Pausing input processes" . PHP_EOL);

                foreach($this->input_processes as $process)
                {
                    if ($process->isRunning()) {
                        $process->terminate(SIGSTOP);
                        fwrite(STDERR, "Stopped input process " . $process->getCommand() . PHP_EOL);
                    }
                }
                $paused = true;
            }
            else
            {
                if ($paused &&
                    $percent_used <= self::MEMORY_PRESSURE_LOW_WATERMARK &&
                    count($this->inflightActionCommands) < self::RUNNING_ACTION_LIMIT_LOW_WATERMARK) {
                    foreach ($this->input_processes as $process) {
                        $process->terminate(SIGCONT);
                        fwrite(STDERR, "Resuming input process " . $process->getCommand() . PHP_EOL);
                    }
                    $paused = false;
                }
            }
        }
    }

    protected function calculateMemoryLimit()
    {
        $multiplierTable = ['K' => 1024, 'M' => 1024**2, 'G' => 1024**3];

        $memory_limit_setting = ini_get('memory_limit');

        if ("-1" == $memory_limit_setting) {
            return -1;
        }

        preg_match("/^(-?[.0-9]+)([KMG])?$/i", $memory_limit_setting, $matches, PREG_UNMATCHED_AS_NULL);

        $bytes = (int)$matches[1];
        $multiplier = (null == $matches[2]) ? 1 : $multiplierTable[strtoupper($matches[2])];

        $bytes = $bytes * $multiplier;

        return $bytes;
    }

    protected function isOpcacheEnabled()
    {
        if (!function_exists('opcache_get_status'))
        {
            return false;
        }
        $status = opcache_get_status(false);

        return (false !== $status);
    }
}
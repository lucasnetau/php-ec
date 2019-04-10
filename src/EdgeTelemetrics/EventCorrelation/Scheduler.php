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

use function array_key_first;
use function fwrite;
use function tempnam;
use function json_encode;
use function json_decode;
use function memory_get_usage;
use function memory_get_peak_usage;

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
     * @var array Configuration for input processes
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

    /** @var string|null */
    protected $newEventAction = null;

    public function __construct(array $rules)
    {
        $this->rules = $rules;
    }

    public function register_input_process($id, string $cmd, ?string $wd, array $env = [])
    {
        $this->input_processes_config[$id] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env];
    }

    public function setup_input_process(Process $process, $id) {
        $process->start($this->loop);
        $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

        $process_decoded_stdout->on('data', function(JsonRpcNotification $rpc) use ($id) {
            switch ( $rpc->getMethod() ) {
                case 'handle':
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
                case 'checkpoint':
                    $this->input_processes_checkpoints[$id] = $rpc->getParams();
                    break;
                default:
                    throw new \RuntimeException("Unknown json rpc command {$rpc->getMethod()} from input process");
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

    public function register_action(string $name, string $cmd, ?string $wd, ?bool $singleShot = false, array $env = [])
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
                     * We need to watch this queue and handle any run-away errors (eg a database been unavailable to ingest events)
                     */
                    $error = ['error' => $response->getError(),
                        'action' => $this->inflightActionCommands[$response->getId()],
                    ];
                    $this->erroredActionCommands[] = $error;
                    unset($this->inflightActionCommands[$response->getId()]);

                    fwrite(STDERR, $response->getError()->getMessage() . PHP_EOL);
                }
            });

            $process->on('exit', function ($code, $term) use ($actionName) {
                /** Action has terminated. If it successfully completed then it will have sent an ack on stdout first before exit */
                if ($term === null) {
                    fwrite(STDERR, "Action $actionName exited with code: $code" . PHP_EOL);
                } else {
                    fwrite(STDERR, "Action $actionName exited on signal: $term" . PHP_EOL);
                }
                unset($this->runningActions[$actionName]);
            });

            $this->runningActions[$actionName] = $process;
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
            if ($this->engine->isDirty())
            {
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
        $file->putContents(json_encode($this->buildState(), JSON_PRETTY_PRINT))
            ->then(function () use ($file) {
                $file->rename($this->saveFileName)
                    ->then(function (\React\Filesystem\Node\FileInterface $newfile) {
                        $this->engine->clearDirtyFlag();
                        echo "State Saved\n";
                    }, function (\Exception $e) {
                        throw $e;
                    });
            }, function (\Exception $e) {
                throw $e;
            });
    }

    public function saveStateSync()
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        file_put_contents($filename, json_encode($this->buildState(), JSON_PRETTY_PRINT));
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
        $this->setState($savedState['scheduler']);

        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);
        $this->engine->setState($savedState['engine']);
    }

    public function run()
    {
        $this->loop = Factory::create();
        fwrite(STDERR, "Using event loop implementation: " . get_class($this->loop) . PHP_EOL);

        /** Restore the state of the scheduler and engine */
        $this->restoreState();

        /** Force a run of the PHP GC and release caches. */
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
                $rpc_request = new JsonRpcRequest();
                $rpc_request->setMethod('run');
                $rpc_request->setParams($action->getVars());
                $id = mt_rand();
                $rpc_request->setId($id);
                $this->inflightActionCommands[$rpc_request->getId()] = $action;
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
        $this->loop->addPeriodicTimer(10, function() {
            fwrite(STDERR, "Current Memory Usage: " . memory_get_usage(true)/1024/1024 . " MB" . PHP_EOL);
            fwrite(STDERR, "Peak Memory Usage: " . memory_get_peak_usage(true)/1024/1024 . " MB" . PHP_EOL);
        });

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
        return json_decode(file_get_contents($this->saveFileName), true);
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
       $this->erroredActionCommands = array_merge($state['actions']['inflight'], $state['actions']['errored']);
    }
}
<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use React\EventLoop\LoopInterface;
use React\ChildProcess\Process;
use React\EventLoop\TimerInterface;

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

    protected $rules = [];

    protected $input_processes_config = [];

    protected $runningActions = [];

    protected $actionConfig = [];

    protected $saveHandler;

    protected $saveFileName = '/tmp/php-ec-savepoint';

    /** @var string|null */
    protected $newEventAction = null;

    public function __construct(array $rules)
    {
        $this->rules = $rules;
    }

    public function register_input_process(string $cmd, ?string $wd, array $env = [])
    {
        $this->input_processes_config[] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env];
    }

    public function setup_input_process(Process $process) {
        $process->start($this->loop);
        /** @todo Don't hardcode stdout decoder */
        $process_decoded_stdout = new \Clue\React\NDJson\Decoder($process->stdout, true);

        $process_decoded_stdout->on('data', function($data) {
            $event = new Event($data);
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
                //@TODO handle any errors
                fwrite(STDERR, $data . "\n");
            });

            $process->stdout->on('data', function ($data) {
                //@TODO handle any acks
                //echo $data . "\n";
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
         * @var $saveHandler
         */
        $filesystem = \React\Filesystem\Filesystem::create($this->loop);
        $this->saveHandler = $this->loop->addPeriodicTimer(1, function() use ($filesystem) {
            if ($this->engine->isDirty())
            {
                $engine = $this->engine;
                $filename = tempnam("/tmp", ".php-ce.state.tmp");
                $file = $filesystem->file($filename);
                $file->putContents(json_encode($this->engine->getState(), JSON_PRETTY_PRINT))
                    ->then(function () use ($file, $engine) {
                        $file->rename($this->saveFileName )
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
        });
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

    public function run()
    {
        $this->loop = \React\EventLoop\Factory::create();

        /** Initialise the Correlation Engine */
        $this->engine = new CorrelationEngine($this->rules);

        /** Start input processes */
        foreach ($this->input_processes_config as $config)
        {
            $process = new Process($config['cmd'], $config['wd'], $config['env']);
            $this->setup_input_process($process);
            $this->input_processes[] = $process;
        }

        /** When new events are emitted from the correlation engine we persist them (hopefully) by sending them through an action */
        if ($this->newEventAction) {
            $this->engine->on('event', function($event) {
                $action = new Action($this->newEventAction, json_decode(json_encode($event),true));
                $this->engine->emit('action', [$action]);
            });
        };

        $this->engine->on('action', function(Action $action) {
            $actionName = $action->getCmd();
            if (isset($this->actionConfig[$actionName]))
            {
                $process = $this->start_action($actionName);
                /** Once the process is up and running we then write out our data via it's STDIN, encoded as JSON */
                $process->stdin->write(json_encode($action->getVars()) . "\n");
            }
            else
            {
                echo "Unknown Action: " . json_encode(($action)) . "\n";
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
            echo "Received SIGHUP but ignoring\n";
        });

        /** @TODO Load State from save file */

        /** GO! */
        $this->loop->run();
    }

    public function stop()
    {
        $this->loop->stop();

        /** @TODO Save final state */
    }
}
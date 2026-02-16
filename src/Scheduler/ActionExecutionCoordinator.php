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

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Error;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\React\Decoder as JsonRpcDecoder;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use JsonSchema\Constraints\Constraint;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LogLevel;
use React\ChildProcess\Process;
use React\EventLoop\Loop;
use React\Promise\Promise;
use Throwable;
use function array_filter;
use function array_key_exists;
use function array_keys;
use function bin2hex;
use function count;
use function is_callable;
use function is_string;
use function json_encode;
use function random_bytes;
use function round;
use function trim;

class ActionExecutionCoordinator implements \Evenement\EventEmitterInterface, LoggerAwareInterface
{
    use \Evenement\EventEmitterTrait;

    use LoggerAwareTrait;

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

    protected \WeakMap $inflightActionClosures;

    /**
     * @var array
     */
    protected array $erroredActionCommands = [];


    public function __construct() {
        $this->inflightActionClosures = new \WeakMap();
    }

    /**
     * @param string $name
     * @param string|array|callable $cmd
     * @param string|null $wd
     * @param bool|null $singleShot
     * @param array $env
     * @param object|array|null $schema Validate action's parameters with a JSONSchema object (json_decode output)
     */
    public function register_action(string $name, string|array|callable $cmd, ?string $wd = null, ?bool $singleShot = false, array $env = [], object|array|null $schema = null) : void
    {
        $this->actionConfig[$name] = ['cmd' => $cmd, 'wd' => $wd, 'env' => $env, 'singleShot' => $singleShot, 'schema' => $schema];
    }

    /**
     * @param string $actionName
     * @return Process
     */
    protected function start_action(string $actionName): Process
    {
        $actionConfig = $this->actionConfig[$actionName];
        /** @TODO: Handle singleShot processes true === $actionConfig['singleShot'] treat as not running for accounting */
        if (!isset($this->runningActions[$actionName])) {
            /** If there is no running action then we initialise the process **/
            if (is_string($actionConfig['cmd'])) {
                /** we call exec to ensure actions can receive our signals and not the default bash wrapper */
                $process = new Process('exec ' . $actionConfig['cmd'], $actionConfig['wd'], $actionConfig['env'] ?? []);
            } else {
                /** When passed an array PHP will call command directly without going through a shell */
                $process = new Process($actionConfig['cmd'], $actionConfig['wd'], $actionConfig['env'] ?? []);
            }
            $process->start(Loop::get());
            $this->emit('process.start', ['actionName' => $actionName]);

            $process->stderr->on('data', function ($data) use ($actionName) {
                $this->logger->error("$actionName message: " . trim($data));
            });

            $process_decoded_stdout = new JsonRpcDecoder( $process->stdout );

            /** Handler for the Json RPC response */
            $process_decoded_stdout->on('data', function ($rpc) use ($actionName) {
                if ($rpc instanceof JsonRpcResponse) {
                    if ($rpc->isSuccess()) {
                        /** Once the action has been processed successfully we can discard of our copy of it */
                        $action = $this->inflightActionCommands[$rpc->getId()];
                        $action->emit('completed', []);
                        unset($this->inflightActionCommands[$rpc->getId()]);
                    } else {
                        /** Transfer the action from the running queue to the errored queue
                         * @TODO We need to watch this queue and handle any run-away errors (eg a database been unavailable to ingest events)
                         * @TODO This should be put into a function as we call it both here and when an action terminates unexpectedly
                         */
                        if (!array_key_exists($rpc->getId(), $this->inflightActionCommands)) {
                            $this->logger->error("Instance of $actionName has already been cleaned up or sent invalid id in response");
                        } else {
                            $error = [
                                'error' => $rpc->getError(),
                                'action' => $this->inflightActionCommands[$rpc->getId()]['action'],
                            ];
                            $this->erroredActionCommands[] = $error;
                            $this->emit('action.error', ['action' => $error['action'], 'error' => $rpc->getError()->getMessage()]);
                            $action = $this->inflightActionCommands[$rpc->getId()];
                            $action->emit('failed', ['exception' => new \RuntimeException($rpc->getError()->getMessage())]);
                            unset($this->inflightActionCommands[$rpc->getId()]);
                        }
                        $this->logger->error($rpc->getError()->getMessage() . " : " . json_encode($rpc->getError()->getData()));
                    }
                    /** Release memory used by the inflight action table */
                    if (empty($this->inflightActionCommands)) {
                        $this->inflightActionCommands = [];

                        $this->emit('action.inflight.idle', ['errorCount' => count($this->erroredActionCommands)]);
                    }
                    $this->emit('dirty');
                } elseif ($rpc instanceof JsonRpcNotification) {
                    if ($rpc->getMethod() === Scheduler::RPC_PROCESS_LOG) {
                        //Log action expects logLevel to match \Psr\Log\LogLevel
                        $this->logger->log($rpc->getParam('logLevel'), $rpc->getParam('message'), $rpc->getParam('context') ?? []);
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
                /** @TODO What happens if code === 0 with actions un-acked? Log message? */
                if ($code !== 0) {
                    /** Go through inflight actions and look for any that match our exiting with error action. Mark them as errored, otherwise they stay in the inflight action commands queue */
                    $pid = $process->getPid();
                    if (null !== $pid)
                    {
                        $terminatedActions = array_filter($this->inflightActionCommands, function($action) use ($pid) { return $pid === $action['pid']; } );
                        $terminatedMessage =  "Action process terminated unexpectedly " . (($term === null) ? "with code: $code" : "on signal: $term");
                        $terminatedException = new \RuntimeException($terminatedMessage);
                        foreach($terminatedActions as $rpcId => $action) {
                            $error = [
                                'error' => [
                                    "message" => $terminatedMessage,
                                ],
                                'action' => $action['action'],
                            ];
                            $this->erroredActionCommands[] = $error;
                            $this->emit('action.error', [
                                'action' => $action['action'],
                                'error' => [
                                    'code' => $code ?? -1,
                                    'message' => $terminatedMessage,
                                ],
                            ]);
                            $action['action']->emit('failed', ['exception' => $terminatedException]);
                            unset($this->inflightActionCommands[$rpcId]);
                        }
                    }

                    $this->emit('process.error', ['actionName' => $actionName, 'error' => $terminatedMessage ?? 'Unknown pid']);
                }
                unset($this->runningActions[$actionName]);
                $this->checkIdle();
                $this->emit('dirty');
            });

            $this->runningActions[$actionName] = $process;
            $this->emit('dirty');
            return $process;
        }

        return $this->runningActions[$actionName];
    }

    public function handleAction(Action $action): void
    {
        //@TODO Implement queue to rate limit execution of actions
        $actionName = $action->getCmd();
        if (isset($this->actionConfig[$actionName])) {
            $config = $this->actionConfig[$actionName];

            //Validate the parameters before calling
            if ($config['schema']) {
                $params = (object)$action->getVars();
                $validator = new \JsonSchema\Validator;
                if ($validator->validate($params, $config['schema'], Constraint::CHECK_MODE_VALIDATE_SCHEMA) !== \JsonSchema\Validator::ERROR_NONE) {
                    $this->logger->error("Invalid parameters for action $actionName", ['errors' => $validator->getErrors()]);
                    $error = [
                        'error' => [
                            'code' => Error::INVALID_PARAMS,
                            'message' => "Invalid parameters for action $actionName",
                        ],
                        'action' => $action,
                    ];
                    $this->erroredActionCommands[] = $error;
                    return;
                }
            }

            if (is_callable($config['cmd'])) {
                $cmd = new ClosureActionWrapper($config['cmd'], $this->logger);
                $action->emit('started');
                $promise = $cmd($action->getVars());
                $this->inflightActionClosures[$promise] = $action->getVars(); //Keep track of promises so we can cancel on shutdown
                $promise->then(function ($result) use ($actionName, $action, $cmd, $promise) {
                    $this->inflightActionClosures->offsetUnset($promise);
                    $this->logger?->debug('Process Output', ['result' => $result]);
                    $action->emit('completed', []);
                    Loop::futureTick($this->checkIdle(...));
                    /** @TODO: Accounting? */
                })->catch(function (Throwable $exception) use ($action, $actionName, $cmd, $promise) {
                    $this->inflightActionClosures->offsetUnset($promise);
                    $this->logger->critical('Callable Action ' . $actionName . ' threw.', ['exception' => $exception]);
                    foreach($this->inflightActionClosures as $closure => $vars) {
                        echo json_encode($vars) . PHP_EOL;
                    }
                    $error = [
                        'error' => [
                            'code' => $exception->getCode(),
                            'message' => $exception->getMessage(),
                        ],
                        'action' => $action,
                    ];
                    $this->erroredActionCommands[] = $error;
                    $this->emit('action.error', [
                        'action' => $action,
                        'error' => [
                            'code' => $exception->getCode(),
                            'message' => $exception->getMessage(),
                        ],
                    ]);
                    $action->emit('failed', ['exception' => $exception]);
                    Loop::futureTick($this->checkIdle(...));
                });
            } else {
                $process = $this->start_action($actionName);
                /** Once the process is up and running we then write out our data via it's STDIN, encoded as a JSON RPC call */
                do {
                    $uniqid = round(hrtime(true)/1e+3) . '.' . bin2hex(random_bytes(4));
                } while (array_key_exists($uniqid, $this->inflightActionCommands));
                $rpc_request = new JsonRpcRequest(Scheduler::ACTION_RUN_METHOD, $action->getVars(), $uniqid);
                $this->inflightActionCommands[$uniqid] = [
                    'action' => $action,
                    'pid' => $process->getPid(),
                ];
                $this->emit('dirty');
                $process->stdin->write(json_encode($rpc_request) . "\n");
                $action->emit('started');
            }
        } else {
            $this->logger->error("Action $actionName is not defined");
            $this->erroredActionCommands[] = [
                'error' => [
                    "code" => 1,
                    "message" => "Unable to start undefined action $actionName"
                ],
                'action' => $action,
            ];
        }
    }

    private function checkIdle() : void {
        /**
         * The runningActions queue array can grow large, using a lot of memory,
         * once it empties we then re-initialise it so that PHP GC can release memory held by the previous array
         */
        echo 'checking idle' . PHP_EOL;
        if (count($this->runningActions) === 0) {
            $this->runningActions = [];
            echo 'no running actions' . PHP_EOL;

            if ($this->inflightActionClosures->count() === 0) {
                echo 'no running closures' . PHP_EOL;
                $this->emit('actionexecutioncoordinator.idle', ['errorCount' => count($this->erroredActionCommands)]);
            } else {
                echo 'running closures' . PHP_EOL;
                foreach($this->inflightActionClosures as $closure => $vars) {
                    echo json_encode($vars) . PHP_EOL;
                }
                echo '.' . PHP_EOL;
            }
        }
    }

    public function isIdle() : bool {
        return count($this->inflightActionCommands) === 0 && $this->inflightActionClosures->count() === 0;
    }

    public function inflightActionCount() : int {
        return count($this->inflightActionCommands);
    }

    public function getInflightActionCommands() : array {
        return $this->inflightActionCommands;
    }

    public function getInflightActionClosures() : array {
        $res = [];
        foreach($this->inflightActionClosures as $closure => $vars) {
            $res[] = $vars;
        }
        return $res;
    }

    public function getErroredActionCommands() : array {
        return $this->erroredActionCommands;
    }

    public function clearErroredActionCommands() : void {
        $this->erroredActionCommands = [];
    }

    public function runningActionCount() : int {
        return count($this->runningActions);
    }

    /**
     * @return string[]
     */
    public function runningActionNames() : array {
        return array_keys($this->runningActions);
    }

    /**
     * Terminate all running processes
     */
    public function shutdown(): void {
        foreach ($this->runningActions as $processKey => $process) {
            /** End the stdin for the process to ensure we flush any pending actions */
            $process->stdin->end();
        }

        Loop::futureTick(function() {
            foreach ($this->runningActions as $processKey => $process) {
                $this->logger->debug("Sending SIGTERM to action process $processKey");
                if (false === $process->terminate(SIGTERM)) {
                    $this->logger->error("Unable to send SIGTERM to action process $processKey");
                }
            }
        });

        Loop::addTimer(10, function() {
            foreach ($this->inflightActionClosures as $promise => $_) {
                /** @var Promise $promise */
                $promise->cancel();
            }
        });
    }
}

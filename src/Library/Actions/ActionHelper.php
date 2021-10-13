<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library\Actions;

use Clue\React\NDJson\Encoder;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\React\Decoder;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use Evenement\EventEmitter;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Stream\ReadableStreamInterface;
use React\Stream\WritableStreamInterface;

use EdgeTelemetrics\JSON_RPC\RpcMessageInterface;
use Psr\Log\LogLevel;
use React\Stream\ReadableResourceStream;
use React\Stream\WritableResourceStream;

use Throwable;
use function EdgeTelemetrics\EventCorrelation\disableOutputBuffering;
use function EdgeTelemetrics\EventCorrelation\rpcLogMessage;
use function error_get_last;
use function fwrite;
use function json_encode;
use function register_shutdown_function;
use function set_exception_handler;

/**
 * Class Action Helper
 * @package EdgeTelemetrics\EventCorrelation
 *
 * This Helper Class can be initialised by Actions. It will set up the STDOUT and STDIN streams to support JSON-RPC to/from the Scheduler
 * ActionHelper will emit two events
 *  * run - Event the action should watch for to handle a RPC notification/method call, a JsonRpcNotification will be passed as data
 *  * shutdown - On receiving this event the action should flush buffers and then call the ActionHelper::stop() function
 */
class ActionHelper extends EventEmitter {

    protected LoopInterface $loop;

    protected WritableStreamInterface $output;

    protected ReadableStreamInterface $input;

    const ACTION_EXECUTE = 'run';

    const ACTION_SHUTDOWN = 'shutdown';

    public function __construct() {
        disableOutputBuffering();
        $this->loop = Loop::get();
        //Register these after the ReactPHP event loop is initialised via Loop::get() to ensure out shutdown function is always processed after the one registered there
        register_shutdown_function(function() {
            $last_error = error_get_last();
            if (($last_error['type'] ?? 0) & (E_ERROR | E_CORE_ERROR | E_COMPILE_ERROR | E_USER_ERROR | E_RECOVERABLE_ERROR)) {
                fwrite(STDOUT, json_encode(rpcLogMessage(LogLevel::EMERGENCY, "Fatal Error ({$last_error['file']}:{$last_error['line']}): {$last_error["message"]}")) . "\n");
            }
        });
        //If any unhandled exception occur then log them to STDOUT (skip the and WritableStreamInterface $output) then terminate the Loop
        set_exception_handler(function (Throwable $exception) {
            fwrite(STDOUT, json_encode(rpcLogMessage(LogLevel::EMERGENCY, "Action terminating on uncaught exception. " . $exception->getMessage() . "\n" . $exception->getTraceAsString())) . "\n");
            Loop::stop();
        });

        $this->input = new Decoder( new ReadableResourceStream(STDIN));
        $this->output = new Encoder( new WritableResourceStream(STDOUT));

        $this->input->on('data', function(JsonRpcNotification $rpc) {
            if (Scheduler::ACTION_RUN_METHOD === $rpc->getMethod()) {
                $this->emit(self::ACTION_EXECUTE, [$rpc]);
            } else {
                if ($rpc instanceof JsonRpcRequest) {
                    $error = new JsonRpcError(JsonRpcError::METHOD_NOT_FOUND,
                        JsonRpcError::ERROR_MSG[JsonRpcError::METHOD_NOT_FOUND]);
                    $response = JsonRpcResponse::createFromRequest($rpc, $error);
                    $this->output->write($response);
                }
            }
        });

        /** Signal Handlers */
        $this->loop->addSignal(SIGINT, function() {
            $this->write(rpcLogMessage(LogLevel::DEBUG, "received SIGINT, flushing..."));
            $this->emit(self::ACTION_SHUTDOWN);
        });
        $this->loop->addSignal(SIGTERM, function() {
            $this->write(rpcLogMessage(LogLevel::DEBUG, "received SIGTERM, flushing..."));
            $this->emit(self::ACTION_SHUTDOWN);
        });
    }

    /**
     * Write JSON-RPC message to Standard Out
     * @param RpcMessageInterface $rpc
     */
    public function write(RpcMessageInterface $rpc) {
        $this->output->write($rpc);
    }

    /**
     * Helper method to start the Event loop
     */
    public function run() {
        $this->loop->run();
    }

    /**
     * Actions should signal they have flushed any buffers and are ready for us to stop by calling this method
     */
    public function stop() {
        $this->loop->futureTick( function() { //Stop in a future tick to allow the stdout streams to flush
            $this->loop->stop();
        });
    }
}
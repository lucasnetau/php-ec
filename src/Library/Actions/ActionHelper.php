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

use function EdgeTelemetrics\EventCorrelation\disableOutputBuffering;
use function EdgeTelemetrics\EventCorrelation\rpcLogMessage;
use function EdgeTelemetrics\EventCorrelation\setupErrorHandling;

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
        setupErrorHandling(true);
        disableOutputBuffering();
        $this->loop = Loop::get();

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
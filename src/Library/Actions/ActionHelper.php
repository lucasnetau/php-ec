<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library\Actions;

use Closure;
use Clue\React\NDJson\Encoder;
use EdgeTelemetrics\EventCorrelation\JsonRpcLogger;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\JSON_RPC\React\Decoder;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use Evenement\EventEmitter;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Stream\ReadableStreamInterface;
use React\Stream\WritableStreamInterface;

use EdgeTelemetrics\JSON_RPC\RpcMessageInterface;
use Psr\Log\LogLevel;
use React\Stream\ReadableResourceStream;
use React\Stream\WritableResourceStream;

use function EdgeTelemetrics\EventCorrelation\disableOutputBuffering;
use function EdgeTelemetrics\EventCorrelation\setupErrorHandling;
use function function_exists;

/**
 * Class Action Helper
 * @package EdgeTelemetrics\EventCorrelation
 *
 * This Helper Class can be initialised by Actions. It will set up the STDOUT and STDIN streams to support JSON-RPC to/from the Scheduler
 * ActionHelper will emit two events
 *  * run - Event the action should watch for to handle an RPC notification/method call, a JsonRpcNotification will be passed as data
 *  * shutdown - On receiving this event the action should flush buffers and then call the ActionHelper::stop() function
 */
class ActionHelper extends EventEmitter {

    protected LoopInterface $loop;

    protected WritableStreamInterface $output;
    protected ReadableStreamInterface $input;

    const ACTION_EXECUTE = 'run';

    const ACTION_SHUTDOWN = 'shutdown';

    protected Closure $signalHandler;

    protected LoggerInterface $logger;

    const INPUT_BUFFER_SIZE = 65536;

    public function __construct(array $options = []) {
        setupErrorHandling(true);
        disableOutputBuffering();
        $this->loop = Loop::get();

        if (function_exists('\cli_set_process_title')){
            $myName = $options['action_name'] ?? 'action process';
            @\cli_set_process_title($myName); //Silence it as macOS prior to certain PHP versions it does not work
        }

        //Drop the process into its own process group so that SIGINT isn't propagated when running under a shell
        if (function_exists('\posix_setpgid')) {
            \posix_setpgid(0,0);
        }

        $buffer_size = $options['json_buffer_size'] ?? self::INPUT_BUFFER_SIZE;

        $this->input = new Decoder(new ReadableResourceStream(STDIN), $buffer_size);
        $this->output = new Encoder(new WritableResourceStream(STDOUT));

        $this->logger = new JsonRpcLogger(LogLevel::DEBUG, STDOUT);

        $this->input->on('error', function($exception) use ($buffer_size) {
            if ($exception instanceof \OverflowException) {
                $this->logger->critical("RPC Request was greater than configured buffer size $buffer_size");
            } else {
                $this->logger->critical('Unexpected exception on STDIN', ['exception' => $exception]);
            }
            $this->stop();
        });

        $this->input->on('data', function(JsonRpcNotification $rpc) {
            if (Scheduler::ACTION_RUN_METHOD === $rpc->getMethod()) {
                try {
                    $this->emit(self::ACTION_EXECUTE, [$rpc]);
                } catch (\Throwable $t) {
                    $this->logger->log(LogLevel::CRITICAL, 'Terminating action process on unhandled exception', ['exception' => $t]);
                    $this->stop();
                }
            } else {
                if ($rpc instanceof JsonRpcRequest) {
                    $error = new JsonRpcError(JsonRpcError::METHOD_NOT_FOUND,
                        JsonRpcError::ERROR_MSG[JsonRpcError::METHOD_NOT_FOUND]);
                    $response = JsonRpcResponse::createFromRequest($rpc, $error);
                    $this->output->write($response);
                }
            }
        });

        //When STDIN closes we can't receive any more data so let's signal to the app to flush and close
        $this->input->on('close', function() {
            $this->emit(self::ACTION_SHUTDOWN);
        });

        $this->output->on('close', function() {
            //Don't try to do anything else, our output has closed, we are either shutting down or we cannot communicate with scheduler
            $this->loop->removeSignal(SIGINT, $this->signalHandler);
            $this->loop->removeSignal(SIGTERM, $this->signalHandler);
            $this->loop->stop();
        });

        $this->signalHandler = function(int $signal) {
            $lookup = [
                SIGINT => 'SIGINT',
                SIGTERM => 'SIGTERM',
                SIGKILL => 'SIGKILL',
            ];
            $this->logger->log(LogLevel::DEBUG, "ActionHelper received signal " . $lookup[$signal] ?? $signal . "finishing up action...");
            $this->loop->futureTick(function () {
                $this->emit(self::ACTION_SHUTDOWN);
            });
        };

        /** Signal Handlers */
        $this->loop->addSignal(SIGINT, $this->signalHandler);
        $this->loop->addSignal(SIGTERM, $this->signalHandler);
    }

    /**
     * Write JSON-RPC message to Standard Out
     * @param RpcMessageInterface $rpc
     * @return bool
     */
    public function write(RpcMessageInterface $rpc) : bool {
        return $this->output->write($rpc);
    }

    /**
     * Helper function to log through to the Scheduler
     * @param string $logLevel
     * @param string $message
     * @param array $context
     * @return void
     */
    public function log(string $logLevel, string $message, array $context = []): void
    {
        $this->logger->log($logLevel, $message, $context);
    }

    /**
     * Helper method to start the Event loop
     */
    public function run() : void {
        $this->loop->run();
    }

    /**
     * Actions should signal they have flushed any buffers and are ready for us to stop by calling this method
     * We shut down by giving input time to deliver all final data and then close input
     */
    public function stop() : void {
        if ($this->input->isReadable()) {
            $this->loop->addTimer(1.0, function () {
                $this->input->close();
            });
        } else if ($this->output->isWritable()) {
            unset($this->logger);
            $this->output->end();
        }
    }
}
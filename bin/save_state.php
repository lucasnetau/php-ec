<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use Psr\Log\LogLevel;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

require_once __DIR__ . '/composer.php';

$filename = getenv('SAVESTATE_FILENAME');
if ($filename === false) {
    fwrite(STDERR, 'No filename configured for save state to be written to. Set environment variable SAVESTATE_FILENAME' . PHP_EOL);
    exit(1);
}

new class($filename) {
    protected ActionHelper $processWrap;

    protected LoopInterface $loop;

    public function __construct(protected string $saveFileName) {
        $directory = dirname($this->saveFileName);

        $this->processWrap = new ActionHelper(['json_buffer_size' => 10485760*2]);

        $this->loop = Loop::get();

        $this->processWrap->on(ActionHelper::ACTION_EXECUTE, function(JsonRpcRequest $request) use ($directory) {
            $state = $request->getParam('state');
            $filename = tempnam($directory, ".php-ce.state.tmp"); //Create the temporary file in the same directory as the final destination to ensure rename is on same filesystem
            if (false === $filename) {
                $this->returnError($request, "Error creating temporary save state file, check filesystem and permissions");
                return;
            }
            $saveStateSize = strlen($state);
            if (!(@file_put_contents($filename, $state) === $saveStateSize && rename($filename, $this->saveFileName))) {
                if (file_exists($filename) && !unlink($filename)) {
                    $this->processWrap->log(LogLevel::WARNING, 'Unable to delete temporary save file');
                }
                $this->returnError($request, "Save state sync failed." . json_encode(error_get_last()));
                return;
            }

            $this->processWrap->write(new JsonRpcResponse($request->getId(), [
                'saveStateBeginTime' => $request->getParam('time'),
                'saveStateSizeBytes' => $saveStateSize,
            ]));
        });

        $this->processWrap->on(ActionHelper::ACTION_SHUTDOWN, function() {
            $this->processWrap->stop();
        });

        /** Explicitly start the loop once we have initialised the action */
        $this->processWrap->run();
    }

    /**
     * Send a JSON RPC error with message to the calling Scheduler
     */
    public function returnError(JsonRpcRequest $request, string $message): void
    {
        $response = JsonRpcResponse::createFromRequest($request);
        $response->setError(new JsonRpcError(JsonRpcError::INTERNAL_ERROR, $message));
        $this->processWrap->write($response);
    }
};

<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

require __DIR__ . '/composer.php';

$filename = getenv('SAVESTATE_FILENAME');
if ($filename === false) {
    fwrite(STDERR, 'No filename configured for save state to be written to. Set environment variable SAVESTATE_FILENAME' . PHP_EOL);
    exit(1);
}

new class($filename) {

    /**
     * @var ActionHelper
     */
    protected ActionHelper $processWrap;

    protected LoopInterface $loop;

    protected string $directory;

    public function __construct(protected string $saveFileName) {
        $this->directory = dirname($this->saveFileName);

        $this->processWrap = new ActionHelper();

        $this->loop = Loop::get();

        $this->processWrap->on(ActionHelper::ACTION_EXECUTE, function(JsonRpcRequest $request) {
            $state = $request->getParam('state');
            $filename = tempnam($this->directory, ".php-ce.state.tmp");
            if (false === $filename) {
                $this->returnError($request, "Error creating temporary save state file, check filesystem");
                return;
            }
            $saveStateSize = strlen($state);
            $saveStateBegin = hrtime(true);
            if (!(@file_put_contents($filename, $state) === $saveStateSize && rename($filename, $this->saveFileName))) {
                if (file_exists($filename) && !unlink($filename)) {
                    $this->processWrap->log(\Psr\Log\LogLevel::WARNING, 'Unable to delete temporary save file');
                }
                $this->returnError($request, "Save state sync failed." . json_encode(error_get_last()));
                return;
            }

            $this->processWrap->write(new JsonRpcResponse($request->getId(), [
                'saveStateLastDuration' => (int)round((hrtime(true) - $saveStateBegin)/1e+6), //Milliseconds
                'saveStateSizeBytes' => $saveStateSize,
            ]));
        });
    }

    public function returnError(JsonRpcRequest $request, $message): void
    {
        $response = JsonRpcResponse::createFromRequest($request);
        $response->setError(new JsonRpcError(JsonRpcError::INTERNAL_ERROR, $message));
        $this->processWrap->write($response);
    }
};

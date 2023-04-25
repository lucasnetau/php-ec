<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Error as JsonRpcError;

use Psr\Log\LogLevel;
use React\EventLoop\Loop;
use function EdgeTelemetrics\EventCorrelation\disableOutputBuffering;
use function EdgeTelemetrics\EventCorrelation\env;

require __DIR__ . '/../../vendor/autoload.php';

disableOutputBuffering();

$logger = new \EdgeTelemetrics\EventCorrelation\JsonRpcLogger(LogLevel::DEBUG);

$log_filename = env('LOG_FILENAME');

$loop = Loop::get();

$stream = new EdgeTelemetrics\JSON_RPC\React\Decoder(new \React\Stream\ReadableResourceStream(STDIN, $loop));

$stream->on('data', function (JsonRpcRequest $rpc) use ($log_filename) {
    $response = JsonRpcResponse::createFromRequest($rpc);

    if (Scheduler::ACTION_RUN_METHOD == $rpc->getMethod()) {
        file_put_contents($log_filename, json_encode($rpc->getParams()) . PHP_EOL, FILE_APPEND | LOCK_EX);
        $response->setResult(true);
    } else {
        $error = new JsonRpcError(JsonRpcError::METHOD_NOT_FOUND,
            JsonRpcError::ERROR_MSG[JsonRpcError::METHOD_NOT_FOUND]);
        $response->setError($error);
    }
    fwrite(STDOUT, json_encode($response) . "\n");
});

$logger->info('Logger started');
$loop->run();
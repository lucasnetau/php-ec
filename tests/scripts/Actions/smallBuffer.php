<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;

require __DIR__ . '/../../../vendor/autoload.php';

$helper = new ActionHelper(['action_name' => 'smallBuffer', 'json_buffer_size' => 1024]);

$helper->on(ActionHelper::ACTION_EXECUTE, function(JsonRpcRequest $rpc) use ($helper) {
    $response = JsonRpcResponse::createFromRequest($rpc);
    $response->setResult(true);
    $helper->write($response);
});

$helper->on(ActionHelper::ACTION_SHUTDOWN, function() use ($helper) {
    $helper->stop();
});

$helper->run();

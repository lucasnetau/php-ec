<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\JSON_RPC\Response as JsonRpcResponse;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;
use Psr\Log\LogLevel;

use function EdgeTelemetrics\EventCorrelation\rpcLogMessage;

require __DIR__ . '/../../../vendor/autoload.php';

/**
 * Echo Action
 * This action will log to the Scheduler the requested parameters
 */
new class() {
    /**
     * @var ActionHelper
     */
    protected ActionHelper $processWrap;

    public function __construct()
    {
        /** Initialise the Action Helper, this will handle the stdin/stdout for the process and also any signals */
        $this->processWrap = new ActionHelper();

        $this->processWrap->on(ActionHelper::ACTION_EXECUTE, function(JsonRpcRequest $rpc) {
            /** Log the rpc method via the Scheduler's built in logging method  */
            $this->processWrap->write(rpcLogMessage(LogLevel::NOTICE, 'Action called : ' . json_encode($rpc->getParams())));

            /** Let the scheduler know we have executed successfully, and it can mark the action as complete */
            $response = JsonRpcResponse::createFromRequest($rpc);
            $response->setResult(true);
            $this->processWrap->write($response);
        });

        /** We have been requested to shut down, this is where we can perform any flushing actions before stopping. We let the Action Helper know we are done by calling stop() **/
        $this->processWrap->on(ActionHelper::ACTION_SHUTDOWN, function() {
            $this->processWrap->stop();
        });
    }
};
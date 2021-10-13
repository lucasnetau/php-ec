<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Library\Actions\ActionHelper;
use EdgeTelemetrics\JSON_RPC\Request as JsonRpcRequest;

require __DIR__ . '/../../vendor/autoload.php';

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
            throw new RuntimeException('Exception test case');
        });

        /** We have been requested to shut down, this is where we can perform any flushing actions before stopping. We let the Action Helper know we are done by calling stop() **/
        $this->processWrap->on(ActionHelper::ACTION_SHUTDOWN, function() {
            $this->processWrap->stop();
        });
    }
};
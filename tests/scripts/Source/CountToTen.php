<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../../../vendor/autoload.php";

$delay = (int)((getenv('DELAY') ?: 0)*1e+6);

for($i = 1;  $i <= 10; $i++) {
    $event = new Event(['event' => 'Count', 'value' => $i]);
    $rpc = new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => $event]);
    fwrite(STDOUT, json_encode($rpc) . "\n");
    usleep($delay);
}

exit(0);
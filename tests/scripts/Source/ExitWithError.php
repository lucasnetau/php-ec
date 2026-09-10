<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../../../vendor/autoload.php";

/** Emits one event then exits with an error to catch erroneous on-demand restarts */
$event = new Event(['event' => 'OnDemandRun']);
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => $event])) . "\n");

exit(1);

<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../../../vendor/autoload.php";

/** On-demand source: emits a Fetched event carrying the value passed via ExecuteSource vars (as env) */
$value = getenv('FUZZ_VALUE');
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => new Event(['event' => 'Fetched', 'value' => $value])])) . "\n");

exit(0);

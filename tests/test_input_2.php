<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../vendor/autoload.php";

$length = 60 * 1000000000; //60 seconds

$start = hrtime(true);

while ((hrtime(true) - $start) < $length) {
    $event = new Event(['event' => 'Test:Event:2','datetime' => (new DateTimeImmutable())->format('c')]);
    $rpc = new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => $event]);
    fwrite(STDOUT, json_encode($rpc) . "\n");
}

fwrite(STDERR, "Finished Input 2\n");
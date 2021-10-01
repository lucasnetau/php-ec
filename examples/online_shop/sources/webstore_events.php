<?php declare(strict_types=1);

use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Event;

use Psr\Log\LogLevel;
use function EdgeTelemetrics\EventCorrelation\disableOutputBuffering;
use function EdgeTelemetrics\EventCorrelation\rpcLogMessage;

require __DIR__ . '/../../../vendor/autoload.php';

error_reporting( E_ALL );
ini_set('display_errors', "off");

disableOutputBuffering();

$ndjson = <<<EOT
{"datetime": "2018-07-26 10:00:01", "event": "shop:order:placed", "orderid": "12345"}
{"datetime": "2018-07-26 10:00:20", "event": "shop:order:notification:customer", "orderid": "12345", "emailid": "4444"}
{"datetime": "2018-07-26 10:00:35", "event": "shop:order:paid", "orderid": "12345"}
{"datetime": "2018-07-26 10:02:01", "event": "shop:order:placed", "orderid": "12344"}
{"datetime": "2018-07-26 10:01:01", "event": "messaging:sendgrid:delivery:sent", "orderid": "12345"}
{"datetime": "2018-07-26 10:02:06", "event": "shop:order:paid", "orderid": "12344"}
{"datetime": "2018-11-06 16:29:00+1100", "event": "shop:order:placed", "orderid": "12343"}
{"datetime": "2018-11-07 13:20:00+1100", "event": "shop:order:placed", "orderid": "12348"}
{"datetime": "2018-11-06 16:35:00+1100", "event": "shop:order:paid", "orderid": "12343"}
{"datetime": "2018-11-07 13:20:00+1100", "event": "shop:order:placed", "orderid": "12349"}
{"datetime": "2018-11-07 13:21:00+1100", "event": "shop:order:paid", "orderid": "12348"}
EOT;

$events = explode("\n", $ndjson);

foreach($events as $event)
{
    fwrite(STDOUT, json_encode(rpcLogMessage(LogLevel::NOTICE, "New event: " . $event)) . "\n");
    $event = new Event(json_decode($event, true));
    $rpc = new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => $event]);
    fwrite(STDOUT, json_encode($rpc) . "\n");
    sleep(1);
}

exit;
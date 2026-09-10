<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../../../vendor/autoload.php";

/** Emits a fuzz corpus of event payloads read from FUZZ_FILE, one JSON-RPC notification each */
$file = getenv('FUZZ_FILE');
if ($file === false) {
    exit(2);
}
$payloads = json_decode((string)file_get_contents($file), true);
if (!is_array($payloads)) {
    exit(2);
}

foreach ($payloads as $payload) {
    fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => $payload])) . "\n");
}

exit(0);

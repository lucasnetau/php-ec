<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;

include __DIR__ . "/../../../vendor/autoload.php";

/** Malformed: no event name */
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => ['datetime' => '2024-01-01T00:00:00Z']])) . "\n");

/** Malformed: unparseable datetime */
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => ['event' => 'Bad', 'datetime' => 'not-a-date']])) . "\n");

/** Malformed: event name is not a string */
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => ['event' => ['nested']]])) . "\n");

/** Malformed: receivedTime is not parseable */
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => ['event' => 'Bad', 'receivedTime' => 12345]])) . "\n");

/** Valid event: scheduler must still process this after dropping the above */
fwrite(STDOUT, json_encode(new JsonRpcNotification(Scheduler::INPUT_ACTION_HANDLE, ['event' => new Event(['event' => 'Good', 'value' => 42])])) . "\n");

exit(0);

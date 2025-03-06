<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchAnyRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRuleContinuously;

include __DIR__ . "/../vendor/autoload.php";

$engine = new CorrelationEngine([
    MatchAnyRule::class,
    MatchOneRule::class,
    MatchOneRuleContinuously::class,
]);

$length = 5 * 1000000000; //5 seconds

$start = hrtime(true);
try {
    while ((hrtime(true) - $start) < $length) {
        $current_msec = (int)((hrtime(true) - $start) / 1e+6);

        $minutes = $current_msec / 60000;
        $left = $current_msec % 60000;
        $seconds = $left / 1000;
        $msec = $left % 1000;

        $timestamp = "2021-06-01T00:" . sprintf("%02d:%02d.%03d", $minutes, $seconds, $msec) . "Z";
        $engine->handle(new Event(['event' => 'Test:Event:1', 'datetime' => $timestamp]));
        $engine->handle(new Event(['event' => 'Test:Event:Single', 'datetime' => $timestamp]));
    }
} catch (Exception $e) {
    error_log("Engine threw an error: " . $e->getMessage());
}

echo json_encode($engine->getState(), JSON_PRETTY_PRINT);
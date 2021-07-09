<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchAnyRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRuleContinuously;

include __DIR__ . "/../vendor/autoload.php";
include __DIR__ . "/Rules/MatchAnyRule.php";

$engine = new CorrelationEngine([
    MatchAnyRule::class,
    MatchOneRule::class,
    MatchOneRuleContinuously::class,
]);

try {
    $engine->handle(new Event(['event' => 'Test:Event:1',]));
    $engine->handle(new Event(['event' => 'Test:Event:Single',]));
} catch (Exception $e) {
    error_log("Engine threw an error: " . $e->getMessage());
}

echo json_encode($engine->getState(), JSON_PRETTY_PRINT);
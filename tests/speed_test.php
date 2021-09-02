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

$length = 60 * 1000000000; //60 seconds

$start = hrtime(true);

while ((hrtime(true) - $start) < $length) {
    $engine->handle(new Event(['event' => 'Test:Event:1','datetime' => (new DateTimeImmutable())->format('c')]));
}

echo json_encode($engine->getState(), JSON_PRETTY_PRINT);
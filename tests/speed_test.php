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

$length = 60 * 1000000000; //60 seconds

$start = hrtime(true);

while ((hrtime(true) - $start) < $length) {
    $engine->handle(new Event(['event' => 'Test:Event:1','datetime' => (new DateTimeImmutable())->format('c')]));
}

$state = $engine->getState();

echo 'Processed ' . $state['statistics']['seen']['Test:Event:1'] . ' events  in 60 seconds' . PHP_EOL;
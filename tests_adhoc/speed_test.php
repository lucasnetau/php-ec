<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchAnyRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRule;
use EdgeTelemetrics\EventCorrelation\tests\Rules\MatchOneRuleContinuously;

require_once __DIR__ . "/../bin/composer.php";
require_once __DIR__ . "/Rules/MatchAnyRule.php";
require_once __DIR__ . "/Rules/MatchOneRule.php";
require_once __DIR__ . "/Rules/MatchOneRuleContinuously.php";

$engine = new CorrelationEngine([
    MatchAnyRule::class,
    MatchOneRule::class,
    MatchOneRuleContinuously::class,
]);

const SECONDS = 20;
$length = SECONDS * 1000000000; //60 seconds

$start = hrtime(true);

while ((hrtime(true) - $start) < $length) {
    $engine->handle(new Event(['event' => 'Test:Event:1','datetime' => (new DateTimeImmutable())->format('c')]));
}

$state = $engine->getState();

echo 'Processed ' . ($state['statistics']['seen']['Test:Event:1']/SECONDS) . ' events / second' . PHP_EOL;
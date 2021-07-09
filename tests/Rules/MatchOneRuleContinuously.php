<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRuleContinuously extends Rule {

    const EVENTS = [['Test:Event:Single']];

    public function nextAcceptedEvents(): array
    {
        return static::EVENTS[0];
    }

    public function complete(): bool
    {
        return false;
    }

    public function fire()
    {
        echo __CLASS__ . " matched exact event event: " . $this->getFirstEvent()->event . "\n";
    }
}
<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRuleContinuously extends Rule {

    const EVENTS = [['Test:Event:1']];

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
        $this->trimEventChain(10);
        //echo __CLASS__ . " matched exact event event: " . $this->getFirstEvent()->event . "\n";
    }
}
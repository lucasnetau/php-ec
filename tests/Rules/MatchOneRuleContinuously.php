<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRuleContinuously extends Rule\MatchSingleContinuously {

    const EVENTS = [['Test:Event:1']];

    public function onProgress(): void
    {
        $this->trimEventChain(10);
    }
}
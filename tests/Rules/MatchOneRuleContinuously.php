<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRuleContinuously extends Rule\MatchSingleContinuously {

    const EVENTS = [['Test:Event:1']];

    use Rule\Traits\NeverTimesOutTrait;

    public function onProgress(): void
    {
        $this->trimEventChain(10);
    }
}
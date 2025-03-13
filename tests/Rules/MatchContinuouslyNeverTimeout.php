<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchContinuouslyNeverTimeout extends Rule\MatchSingleContinuously {

    const EVENTS = [['Test:Event:1']];

    use Rule\Traits\NeverTimesOutTrait;

    public function onProgress(): void
    {
        $this->trimEventChain(1);
    }
}
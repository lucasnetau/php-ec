<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchContinuouslyTillTimeout extends Rule\MatchSingleContinuously {

    const EVENTS = [['Test:Event:Once']];

    public function onProgress(): void
    {
        if (!isset($this->context['event'])) {
            $this->context['event'] = $this->getLastEvent();
        }
        $this->trimEventChain(1);
    }

    function alarm(): void
    {
        parent::alarm();
        error_log('Alarm called');
    }

    public function updateTimeout(): void
    {
        if ($this->timeout === null) {
            $this->timeout = $this->context['event']?->datetime->modify('+1 second');
        }
    }
}
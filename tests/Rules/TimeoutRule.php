<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests\Rules;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;

class TimeoutRule extends Rule\MatchSingleContinuously {

    const EVENTS = [[Scheduler::CONTROL_MSG_NEW_STATE]];

    const TIMEOUT = 'PT1S';

    protected array $called = [];

    public function alarm(): void
    {
        $this->called[] = 'alarm called';
        parent::alarm();
    }

    public function onTimeout() : void
    {
        $this->called[] = 'timeout called';
        $action = new Action("recordTimeout", $this->called);
        $this->emit('data', [$action]);
    }
}
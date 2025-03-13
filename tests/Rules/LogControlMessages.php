<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Scheduler;

class LogControlMessages extends Rule\MatchSingle {

    const EVENTS = [[Scheduler::CONTROL_MSG_HEARTBEAT, Scheduler::CONTROL_MSG_NEW_STATE, Scheduler::CONTROL_MSG_RESTORED_STATE, Scheduler::CONTROL_MSG_STOP]];

    public function onComplete() : void
    {
        $action = new Action("log", $this->getFirstEvent());
        $this->emit('data', [$action]);
    }
}
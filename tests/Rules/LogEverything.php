<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Rule;

class LogEverything extends Rule {

    const EVENTS = [[self::EVENT_MATCH_ANY]];

    public function fire()
    {
        $action = new Action("log", $this->consumedEvents[0]);
        $this->emit('data', [$action]);
    }
}

<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Rule;

class LogEverything extends Rule\MatchSingle {

    const EVENTS = [[self::EVENT_MATCH_ANY]];

    public function onComplete() : void
    {
        $action = new Action("log", $this->getFirstEvent());
        $this->emit('data', [$action]);
    }
}
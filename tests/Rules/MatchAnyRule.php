<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchAnyRule extends Rule {

    const EVENTS = [[self::EVENT_MATCH_ANY]];

    public function fire()
    {
        //echo __CLASS__ . " matched any event: " . $this->getFirstEvent()->event . "\n";
    }
}

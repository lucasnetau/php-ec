<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchAnyRule extends Rule\MatchSingle {

    const EVENTS = [[self::EVENT_MATCH_ANY]];
}

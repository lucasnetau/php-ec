<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRule extends Rule\MatchSingle {

    const EVENTS = [['Test:Event:Single']];

}
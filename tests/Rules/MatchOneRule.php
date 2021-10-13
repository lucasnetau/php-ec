<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\tests\Rules;

use EdgeTelemetrics\EventCorrelation\Rule;

class MatchOneRule extends Rule {

    const EVENTS = [['Test:Event:Single']];

    public function fire()
    {
        //echo __CLASS__ . " matched exact event event: " . $this->getFirstEvent()->event . "\n";
    }
}
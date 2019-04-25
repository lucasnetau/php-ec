<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator;

abstract class Rule extends AEventProcessor implements IActionGenerator
{
    /**
     * Check if the values is within the range inclusive of the boundary values
     */
    protected function rangeInclusiveCheck($value, $min, $max)
    {
        return ($value - $min) * ($max - $value) >= 0;
    }

    /**
     * Check if the values is within the range exclusive of the boundary values
     */
    protected function rangeExclusiveCheck($value, $min, $max)
    {
        return ($value - $min) * ($max - $value) > 0;
    }
}
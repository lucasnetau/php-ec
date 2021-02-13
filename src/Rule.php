<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation;

use RuntimeException;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use EdgeTelemetrics\EventCorrelation\StateMachine\IActionGenerator;

abstract class Rule extends AEventProcessor implements IActionGenerator
{
    /**
     * Check if the values is within the range inclusive of the boundary values
     * @param $value
     * @param $min
     * @param $max
     * @return bool
     */
    protected function rangeInclusiveCheck($value, $min, $max) : bool
    {
        if (! is_numeric($value) || ! is_numeric($min) || !is_numeric($max) ) {
            return false;
        }
        return ($value - $min) * ($max - $value) >= 0;
    }

    /**
     * Check if the values is within the range exclusive of the boundary values
     * @param $value
     * @param $min
     * @param $max
     * @return bool
     */
    protected function rangeExclusiveCheck($value, $min, $max) : bool
    {
        if (! is_numeric($value) || ! is_numeric($min) || !is_numeric($max) ) {
            return false;
        }
        return ($value - $min) * ($max - $value) > 0;
    }
}
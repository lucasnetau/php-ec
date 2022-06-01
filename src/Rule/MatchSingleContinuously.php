<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Rule;

use EdgeTelemetrics\EventCorrelation\Rule;

/**
 * Class MatchSingleContinuously
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */
abstract class MatchSingleContinuously extends Rule
{
    const EVENTS = [[]];

    public function complete() : bool
    {
        return false;
    }

    public function nextAcceptedEvents() : array
    {
        /* Continue to accept the same event type */
        return static::EVENTS[0];
    }
}
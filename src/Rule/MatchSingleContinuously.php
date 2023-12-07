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
use function count;

/**
 * Class MatchSingleContinuously
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */
abstract class MatchSingleContinuously extends Rule
{
    public function __construct()
    {
        parent::__construct();
        if (count(static::EVENTS) !== 1) {
            throw new \RuntimeException('A rule of MatchSingleContinuously must have only one event group defined, ' . count(static::EVENTS) . ' are defined');
        }
    }

    public function complete() : bool
    {
        return false;
    }

    /**
     * @return string[]
     */
    public function nextAcceptedEvents() : array
    {
        /* Continue to accept the same event type */
        return static::EVENTS[0];
    }
}
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

use EdgeTelemetrics\EventCorrelation\Scheduler;

/**
 * Class OnStartup
 * @package EdgeTelemetrics\EventCorrelation\Rule
 */
abstract class OnHeartbeat extends MatchSingle
{
    const EVENTS = [[Scheduler::CONTROL_MSG_HEARTBEAT]];
}
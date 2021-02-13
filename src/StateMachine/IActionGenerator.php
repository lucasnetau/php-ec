<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\StateMachine;

use Evenement\EventEmitterInterface;

/**
 * Interface IActionGenerator
 * @package EdgeTelemetrics\EventCorrelation\StateMachine
 *
 * Interface declaring that we emit actions to be executed.
 */
interface IActionGenerator extends EventEmitterInterface {}
<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\StateMachine;

/**
 * Interface IActionGenerator
 * @package EdgeTelemetrics\EventCorrelation\StateMachine
 *
 * Interface declaring that we emit actions to be executed.
 */
interface IActionGenerator extends \Evenement\EventEmitterInterface {}
<?php declare(strict_types=1);

namespace Lucas\EventCorrelation\StateMachine;

/**
 * Interface IActionGenerator
 * @package Lucas\EventCorrelation\StateMachine
 *
 * Interface declaring that we emit actions to be executed.
 */
interface IActionGenerator extends \Evenement\EventEmitterInterface {}
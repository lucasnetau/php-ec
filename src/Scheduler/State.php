<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Scheduler;

//enum State implements \BackedEnum {

class State {

    public const STARTING = 'starting';
    public const RECOVERY = 'recovery';
    public const RUNNING = 'running';
    public const STOPPING = 'stopping';

    public const STOPPED = 'stopped';

    public const STOPPED_UNCLEAN = 'unclean shutdown';

    public const VALID_STATES = [self::STARTING, self::RECOVERY, self::RUNNING, self::STOPPING, self::STOPPED, self::STOPPED_UNCLEAN];

    private string $state;

    public function __construct(string $state) {
        if (in_array($state, self::VALID_STATES, true)) {
            $this->state = $state;
        } else {
            throw new \RuntimeException('Invalid state ' . $state);
        }
    }

    public function state() : string {
        return $this->state;
    }

    public function isStopping() : bool {
        return match($this->state) {
            self::STOPPING => true,
            default => false
        };
    }
}
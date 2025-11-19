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

use EdgeTelemetrics\EventCorrelation\Event;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use RuntimeException;
use function func_num_args;
use function getenv;

abstract class SourceFunction implements LoggerAwareInterface, EventEmitterInterface
{
    /** We emit events */
    use EventEmitterTrait;

    use LoggerAwareTrait;

    protected bool $running = false;
    protected LoopInterface $loop;

    /** @var array The environment for this source function */
    private array $env;

    public function __construct() {
        $this->loop = Loop::get();
    }

    /**
     * @var null|array $env Set the environment for the source function or inherit if null
     */
    public function start(null|array $env = null, mixed $checkpoint = null): void
    {
        $this->running = true;
        $this->env = $env ?? getenv();
        $this->functionStart($checkpoint);
    }

    public function terminate($signal): void
    {
        if ($this->running === false) {
            return;
        }
        $this->functionStop();
    }

    public function isStopped() : bool {
        return !$this->running;
    }

    protected function env(string $variableName, ?string $defaultValue = null): ?string
    {
        // Only mark as optional if the default value was *explicitly* provided.
        $isOptional = (2 === func_num_args());

        $env = $this->env[$variableName] ?? false;
        if ( false === $env ) {
            if ( true === $isOptional ) {
                $env = $defaultValue;
            } else {
                throw new RuntimeException("Non-Optional ENV variable $variableName not set" );
            }
        }

        return $env;
    }

    //Emit the checkpoint data to the scheduler
    public function checkpoint($checkpoint) : void {
        $this->emit('checkpoint', [$checkpoint]);
    }

    public function emitEvent(Event $event) {
        $this->emit('data', [$event]);
    }

    abstract function functionStart(mixed $checkpoint) : void;

    abstract function functionStop() : void;

}

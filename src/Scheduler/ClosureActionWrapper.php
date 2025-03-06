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

use React\Promise\Promise;
use RuntimeException;
use Closure;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;

class ClosureActionWrapper implements LoggerAwareInterface {
    /** PSR3 logger provides $this->logger */
    use LoggerAwareTrait;

    private Closure $closure;

    public function __construct(callable $callback, LoggerInterface $logger) {
        $this->setLogger($logger);

        if ($callback instanceof LoggerAwareInterface) {
            $callback->setLogger($this->logger);
        }

        $callable = $callback(...);
        $bound = @$callable->bindTo($this, $this); //Bind the closure if we can to this instance of ClosureActionWrapper giving access to the Logger
        $this->closure = $bound ?? $callable;
    }

    public function __invoke(array $args): Promise
    {
        $resolver = function (callable $resolve, callable $reject) use ($args) {
            try {
                $closure = $this->closure;
                $resolve($closure($args));
            } catch (\Throwable $e) {
                $reject(new RuntimeException($e->getMessage(), $e->getCode(), $e));
            }
        };

        $canceller = function () {
            // Cancel/abort any running operations like network connections, streams etc.

            // Reject promise by throwing an exception
            throw new RuntimeException('Promise cancelled');
        };

        return new Promise($resolver, $canceller);
    }
}
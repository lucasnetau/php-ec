<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler\ClosureActionWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\Promise;
use RuntimeException;
use Throwable;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ClosureActionWrapper
 */
class ClosureActionWrapperTest extends TestCase {

    private function await(Promise $promise) : mixed {
        $result = null;
        $error = null;
        $promise->then(
            function ($value) use (&$result) { $result = $value; },
            function (Throwable $exception) use (&$error) { $error = $exception; }
        );

        Loop::addTimer(0.1, Loop::stop(...));
        Loop::run();

        if ($error !== null) {
            throw $error;
        }
        return $result;
    }

    public function testInvokesFunctionCallable() : void {
        $wrapper = new ClosureActionWrapper('strlen', new TestLogger());
        $this->assertSame(5, $this->await($wrapper(['hello'])));
    }

    public function testInvokesStaticClosure() : void {
        $wrapper = new ClosureActionWrapper(static fn(int $value) => $value + 1, new TestLogger());
        $this->assertSame(43, $this->await($wrapper([42])));
    }

    public function testInvokesInvokableObject() : void {
        $callable = new class() {
            public function __invoke(int $value) : int {
                return $value * 2;
            }
        };
        $wrapper = new ClosureActionWrapper($callable, new TestLogger());
        $this->assertSame(42, $this->await($wrapper([21])));
    }

    public function testClosureReturningPromiseIsResolved() : void {
        $wrapper = new ClosureActionWrapper(function () {
            return new Promise(function (callable $resolve) {
                $resolve('chained');
            });
        }, new TestLogger());

        $this->assertSame('chained', $this->await($wrapper([])));
    }

    public function testClosureThrowingRejectsWithWrappedException() : void {
        $wrapper = new ClosureActionWrapper(function () {
            throw new RuntimeException('boom');
        }, new TestLogger());

        $rejected = null;
        $wrapper([])->then(null, function (Throwable $exception) use (&$rejected) {
            $rejected = $exception;
        });
        Loop::addTimer(0.1, Loop::stop(...));
        Loop::run();

        $this->assertInstanceOf(RuntimeException::class, $rejected);
        $this->assertStringContainsString('threw: boom', $rejected->getMessage());
    }

    public function testLoggerAwareCallableReceivesLogger() : void {
        $callable = new class() implements LoggerAwareInterface {
            use \Psr\Log\LoggerAwareTrait;

            public function __invoke() : LoggerInterface {
                return $this->logger;
            }
        };

        $logger = new TestLogger();
        $wrapper = new ClosureActionWrapper($callable, $logger);

        $this->assertSame($logger, $this->await($wrapper([])));
    }
}

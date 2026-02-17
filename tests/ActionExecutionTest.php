<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Action;
use EdgeTelemetrics\EventCorrelation\Library\Actions\InvokableClassAction;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\State;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function count;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler
 * @covers \EdgeTelemetrics\EventCorrelation\Action
 * @covers \EdgeTelemetrics\EventCorrelation\Library\Actions\InvokableClassAction
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\ClosureActionWrapper
 */
class ActionExecutionTest extends TestCase {

    public function buildObservableScheduler() : ObservableScheduler {
        $scheduler = new ObservableScheduler([]);
        $tempState = tempnam(sys_get_temp_dir(), 'php-ec-test-state-');
        if ($tempState !== false) {
            unlink($tempState);
        }
        $scheduler->setSavefileName($tempState);
        $scheduler->setSaveStateInterval(600);
        return $scheduler;
    }

    public function testExecutionOfClosureTypeAction(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $wasCalled = false;
        $closure = function() use (&$wasCalled) {
            $wasCalled = true;
            $this->logger->info('ClosureAction ran and able to log');
        };

        $scheduler->register_action('closureAction', $closure);

        Loop::futureTick(function() use ($scheduler, $logger) {
            $scheduler->queueAction(new Action('closureAction', []));
            Loop::futureTick(function() use ($scheduler, $logger) {
                $scheduler->shutdown();
            });
        });

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('ClosureAction ran and able to log');

        $this->assertTrue($wasCalled, 'closureAction was not called');
    }

    /**
     * @covers \EdgeTelemetrics\EventCorrelation\Library\Actions\InvokableClassAction
     */
    public function testExecutionOfInvokableClassTypeAction(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $invokableClass = new class() extends InvokableClassAction {
            public bool $wasCalled = false;
            public function __invoke(): void {
                $this->logger->info("InvokableClassAction ran and able to log");
                $this->wasCalled = true;
            }
        };

        $scheduler->register_action('invokableClassAction', $invokableClass);

        Loop::futureTick(function() use ($scheduler, $logger) {
            $scheduler->queueAction(new Action('invokableClassAction', []));
            Loop::futureTick(function() use ($scheduler, $logger) {
                $scheduler->shutdown();
            });
        });

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('InvokableClassAction ran and able to log');

        $this->assertTrue($invokableClass->wasCalled, 'InvokableClassAction was not called');
    }

    /**
     * @small
     */
    public function testExecutionOfProcessTypeAction(): void
    {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_action('scriptAction',
            \EdgeTelemetrics\EventCorrelation\php_cmd(__DIR__ . '/scripts/Actions/logToScheduler.php'));

        $scheduler->setHandleActionCallback(null, static function() use ($scheduler) { Loop::addTimer(0.4, $scheduler->shutdown(...)); });

        Loop::futureTick(function () use ($scheduler, $logger) {
            $scheduler->queueAction(new Action('scriptAction', ['scriptAction ran and able to log']));
        });

        $scheduler->run();

        $logger->assert()
            ->hasLog()
            ->withMessage('Action called : ["scriptAction ran and able to log"]');
    }

    public function testClosureTypeActionThatThrowsIsGracefullyHandled(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $closure = function() use (&$wasCalled) {
            throw new \RuntimeException('closureAction threw');
        };

        $scheduler->register_action('closureAction', $closure);

        Loop::futureTick(function() use ($scheduler, $logger) {
            $scheduler->queueAction(new Action('closureAction', []));
            Loop::futureTick(function() use ($scheduler, $logger) {
                $scheduler->shutdown();
            });
        });

        $scheduler->run();

        $this->assertEquals(State::STOPPED, $scheduler->getExecutionState()->state());

        $erroredActions = $scheduler->getErroredActions();
        $this->assertCount(1, $erroredActions);
        $this->assertEquals('closureAction threw',  $erroredActions[0]['error']['message']);
    }

    /** @small */
    public function testProcessTypeActionHelperThatExitsUncleanlyMarkedAsErrored(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $scheduler->register_action('scriptAction',
            \EdgeTelemetrics\EventCorrelation\php_cmd(__DIR__ . '/scripts/Actions/exception.php'));

        Loop::futureTick(function () use ($scheduler, $logger) {
            $scheduler->queueAction(new Action('scriptAction', []));

            $shutdownTimer = Loop::addPeriodicTimer(0.5, function () use ($scheduler, $logger, &$shutdownTimer) {
                if (count($scheduler->getErroredActions()) > 0) {
                    Loop::cancelTimer($shutdownTimer);
                    $scheduler->exit();
                }
            });
        });

        $scheduler->run();

        $this->assertEquals(State::STOPPED, $scheduler->getExecutionState()->state());

        $erroredActions = $scheduler->getErroredActions();

        $this->assertCount(1, $erroredActions);
        $this->assertEquals('Action process terminated unexpectedly with code: 1',  $erroredActions[0]['error']['message']);

        $logger->assert()
            ->hasLog()
             ->withMessageStartsWith('RuntimeException: Exception test case in ');
    }

    public function testActionSchemaValidation(): void {
        $scheduler = $this->buildObservableScheduler();

        $logger = new TestLogger();
        $scheduler->setLogger($logger);

        $wasCalled = [];
        $closure = function($test) use (&$wasCalled) {
            $wasCalled[] = $test;
        };

        $scheduler->register_action('closureAction', $closure, schema: [
            "type" => "object",
            "properties" => [
                "test" => [
                    "type" => "boolean",
                ],
            ],
            "required" => ["test"],
            "additionalProperties" => false,
        ]);

        Loop::get()->futureTick(function() use ($scheduler) {
            $scheduler->queueAction(new Action('closureAction', ['test' => true,]));
            $scheduler->queueAction(new Action('closureAction', ['test' => "test",])); //Invalid
            $scheduler->queueAction(new Action('closureAction', ['abc' => true,]));
            Loop::futureTick(function() use ($scheduler) {
                $scheduler->shutdown();
            });
        });

        $scheduler->run();

        $this->assertCount(1, $wasCalled, 'Validation did not pass');
    }
}
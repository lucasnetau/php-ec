<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler\SourceExecutionCoordinator;
use EdgeTelemetrics\EventCorrelation\Scheduler\SourceFunction;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\SourceExecutionCoordinator
 */
class SourceExecutionCoordinatorTest extends TestCase {

    public function testOnDemandProcessIsNotRestartedOnErrorExit() : void {
        $coordinator = new SourceExecutionCoordinator(Loop::get());
        $coordinator->setLogger(new TestLogger());

        $coordinator->register_input_process(
            'ondemand',
            wrap_source_php_cmd(__DIR__ . '/scripts/Source/ExitWithError.php', true),
            autostart: false
        );

        $runs = 0;
        $allStopped = false;
        $coordinator->on('event', function() use (&$runs) { $runs++; });
        $coordinator->on('all_processes_stopped', function() use (&$allStopped) { $allStopped = true; });

        $coordinator->initialise_input_process('ondemand');

        Loop::addTimer(1.0, function() use ($coordinator) {
            $coordinator->setStopping(true);
            Loop::stop();
        });
        Loop::run();

        $this->assertSame(1, $runs, 'On-demand source must only run once');
        $this->assertFalse($allStopped, 'On-demand completion must not signal all processes stopped');
    }

    public function testPauseAndResumeHandleSourceFunctions() : void {
        $coordinator = new SourceExecutionCoordinator(Loop::get());
        $coordinator->setLogger(new TestLogger());

        $source = new class extends SourceFunction {
            public int $stopCalls = 0;

            public function functionStart(mixed $checkpoint): void {}

            public function functionStop(): void {
                $this->stopCalls++;
            }
        };

        $coordinator->register_input_process('source-fn', $source);
        $coordinator->initialise_input_processes();
        Loop::run();

        $runningSource = $coordinator->getInputProcesses()['source-fn'] ?? null;
        $this->assertInstanceOf(SourceFunction::class, $runningSource);
        $this->assertTrue($runningSource->isRunning());

        $coordinator->pauseAllProcesses();
        $this->assertTrue($runningSource->isRunning(), 'Pausing processes must not stop a source function');

        $coordinator->resumeAllProcesses();
        $this->assertTrue($runningSource->isRunning());

        $coordinator->shutdown();
        $this->assertTrue($runningSource->isStopped());
        $this->assertSame(1, $runningSource->stopCalls);
    }

    public function testUnknownProcessIdThrows() : void {
        $coordinator = new SourceExecutionCoordinator(Loop::get());
        $coordinator->setLogger(new TestLogger());

        $this->expectException(\RuntimeException::class);
        $coordinator->initialise_input_process('does-not-exist');
    }
}

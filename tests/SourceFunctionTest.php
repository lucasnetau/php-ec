<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\Messages\ExecuteSource;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Scheduler\SourceFunction;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use RuntimeException;
use Throwable;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\SourceFunction
 */
class SourceFunctionTest extends TestCase {

    private function newSource() : SourceFunction {
        return new class extends SourceFunction {
            public mixed $checkpoint = null;
            public array $envValues = [];
            public bool $missingEnvThrew = false;
            public int $stopCalls = 0;

            public function functionStart(mixed $checkpoint): void {
                $this->checkpoint = $checkpoint;
                try {
                    $this->envValues['set'] = $this->env('MY_VAR');
                } catch (RuntimeException) {
                    $this->envValues['set'] = null;
                }
                $this->envValues['default'] = $this->env('MISSING_VAR', 'default');
                try {
                    $this->env('MISSING_VAR');
                } catch (RuntimeException) {
                    $this->missingEnvThrew = true;
                }
            }

            public function functionStop(): void {
                $this->stopCalls++;
            }
        };
    }

    public function testStartSetsEnvAndCheckpoint() : void {
        $source = $this->newSource();
        $source->start(['MY_VAR' => 'value'], 'cp-1');

        $this->assertFalse($source->isStopped());
        $this->assertSame('cp-1', $source->checkpoint);
        $this->assertSame(['set' => 'value', 'default' => 'default'], $source->envValues);
        $this->assertTrue($source->missingEnvThrew, 'missing env without a default must throw');
    }

    public function testEmitEventAndCheckpoint() : void {
        $source = $this->newSource();
        $events = [];
        $checkpoints = [];
        $source->on('data', function (Event $event) use (&$events) { $events[] = $event; });
        $source->on('checkpoint', function ($checkpoint) use (&$checkpoints) { $checkpoints[] = $checkpoint; });

        $source->start([]);
        $source->emitEvent(new Event(['event' => 'Test']));
        $source->checkpoint('cp-2');

        $this->assertCount(1, $events);
        $this->assertSame('Test', $events[0]->getEventName());
        $this->assertSame(['cp-2'], $checkpoints);
    }

    public function testTerminateStopsAndCallsFunctionStopOnce() : void {
        $source = $this->newSource();
        $source->start([]);

        $source->terminate(SIGTERM);

        $this->assertTrue($source->isStopped());
        $this->assertSame(1, $source->stopCalls);

        $source->terminate(SIGTERM);
        $this->assertSame(1, $source->stopCalls, 'stopping an already stopped source must be a no-op');
    }

    public function testPauseAndResumeSignalsDoNotStopSource() : void {
        $source = $this->newSource();
        $source->start([]);

        $source->terminate(SIGSTOP);
        $this->assertFalse($source->isStopped(), 'SIGSTOP must not stop a source function');
        $this->assertSame(0, $source->stopCalls);

        $source->terminate(SIGCONT);
        $this->assertFalse($source->isStopped());

        $source->terminate(SIGTERM);
        $this->assertTrue($source->isStopped());
    }

    public function testOnDemandSourceFunctionRunsFromExecuteSourceMessage() : void {
        $rule = new class() extends \EdgeTelemetrics\EventCorrelation\Rule\MatchSingle {
            const EVENTS = [[Scheduler::CONTROL_MSG_NEW_STATE]];

            public function onComplete() : void {
                $this->emit('data', [new ExecuteSource('fnsource', ['TEST_VAR' => '42'])]);
            }
        };

        $source = new class extends SourceFunction {
            public function functionStart(mixed $checkpoint): void {
                $this->emitEvent(new Event(['event' => 'FnFetched', 'value' => $this->env('TEST_VAR', 'missing')]));
                $this->running = false;
                $this->emit('exit', [0]);
            }

            public function functionStop(): void {
                $this->running = false;
            }
        };

        $saveFile = tempnam(sys_get_temp_dir(), 'php-ec-fn-source-');
        @unlink($saveFile);
        try {
            $scheduler = new ObservableScheduler([$rule]);
            $scheduler->setLogger(new TestLogger());
            $scheduler->setSavefileName($saveFile);
            $scheduler->setSaveStateInterval(600);
            $scheduler->register_input_process('fnsource', $source, autostart: false);

            $received = [];
            $scheduler->setHandleEventCallback(function ($event) use (&$received, $scheduler) {
                if ($event->event === 'FnFetched') {
                    $received[] = $event->value;
                    Loop::futureTick($scheduler->shutdown(...));
                }
            }, null);

            Loop::addTimer(5, $scheduler->exit(...));
            $scheduler->run();

            $this->assertSame(['42'], $received, 'ExecuteSource vars must reach the on-demand SourceFunction');
        } finally {
            @unlink($saveFile);
        }
    }
}

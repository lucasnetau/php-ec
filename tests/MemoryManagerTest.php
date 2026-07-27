<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler\MemoryManager;
use EdgeTelemetrics\EventCorrelation\SysInfo;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\MemoryManager
 */
class MemoryManagerTest extends TestCase {

    private ?MemoryManager $manager = null;

    /**
     * MemoryManager::check() registers periodic timers on the ReactPHP loop.
     * Cancel them so the test process can exit cleanly.
     */
    protected function tearDown(): void
    {
        if ($this->manager !== null) {
            $this->manager->stop();
        }
    }

    /**
     * Test double that allows controlling memory usage and the pressure
     * watchdog interval without waiting 300 seconds.
     */
    private function buildManager(
        int &$inflightCount,
        bool &$paused,
        bool &$resumed,
        bool &$shutdownCalled,
        ?TestLogger $logger = null,
    ): MemoryManager {
        $loop = Loop::get();

        $this->manager = new class(
            $loop,
            function() use (&$inflightCount) { return $inflightCount; },
            function() use (&$paused) { $paused = true; },
            function() use (&$resumed) { $resumed = true; },
            function() use (&$shutdownCalled) { $shutdownCalled = true; },
            50,   // highWatermark
            35,   // lowWatermark
            30000,// actionLimitHigh
            500,  // actionLimitLow
        ) extends MemoryManager {
            private int $fakeUsage = 0;

            public function setMemoryLimitForTest(int $limit): void
            {
                $this->memoryLimit = $limit;
            }

            public function setMemoryUsageForTest(int $usage): void
            {
                $this->fakeUsage = $usage;
            }

            public function setPressureCheckIntervalForTest(float $interval): void
            {
                $this->pressureCheckInterval = $interval;
            }

            protected function currentMemoryUsage(): int
            {
                return $this->fakeUsage;
            }
        };

        if ($logger !== null) {
            $this->manager->setLogger($logger);
        }

        return $this->manager;
    }

    public function testUnlimitedMemorySkipsCheck(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(SysInfo::NO_LIMIT);

        $manager->check();

        $this->assertFalse($paused, 'Sources should not be paused when memory is unlimited');
        $this->assertSame(0, $manager->getPercentUsed(), 'Percent used should remain 0 when check is skipped');
    }

    public function testReclaimRunsGcAndLogs(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $logger = new TestLogger();
        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled, $logger);

        $manager->reclaim();

        $logger->assert()
            ->hasLog()
            ->withMessageContains('GC Run Complete');
    }

    public function testHighMemoryPressurePausesSources(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(1000);
        $manager->setMemoryUsageForTest(600); // 60% > 50% high watermark

        $manager->check();

        $this->assertTrue($paused, 'Sources should be paused at 60% memory (above 50% high watermark)');
        $this->assertTrue($manager->isPaused());
        $this->assertSame(1, $manager->getPausedCount());
        $this->assertSame(60, $manager->getPercentUsed());
    }

    public function testLowWatermarkResumesSources(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(1000);

        // First trigger a pause
        $manager->setMemoryUsageForTest(600); // 60% > 50%
        $manager->check();
        $this->assertTrue($manager->isPaused());

        // Then drop below the low watermark
        $resumed = false;
        $manager->setMemoryUsageForTest(200); // 20% < 35% low watermark
        $manager->check();

        $this->assertTrue($resumed, 'Sources should be resumed when memory drops below low watermark');
        $this->assertFalse($manager->isPaused());
    }

    public function testHighActionCountPausesSources(): void {
        $inflightCount = 30001;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(PHP_INT_MAX);
        $manager->setMemoryUsageForTest(0); // Memory is fine, but action count exceeds limit

        $manager->check();

        $this->assertTrue($paused, 'Sources should be paused when inflight actions exceed high watermark');
        $this->assertTrue($manager->isPaused());
        $this->assertSame(1, $manager->getPausedCount());
    }

    public function testLowActionCountResumesSources(): void {
        $inflightCount = 30001;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(PHP_INT_MAX);

        // Trigger pause via high action count
        $manager->setMemoryUsageForTest(0);
        $manager->check();
        $this->assertTrue($manager->isPaused());

        // Drop action count below low watermark and memory stays low
        $inflightCount = 100; // < 500 low watermark
        $resumed = false;
        $manager->check();

        $this->assertTrue($resumed, 'Sources should be resumed when action count drops below low watermark');
        $this->assertFalse($manager->isPaused());
    }

    public function testPressureStuckTriggersShutdown(): void {
        $inflightCount = 30001;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(PHP_INT_MAX);
        $manager->setMemoryUsageForTest(0);
        $manager->setPressureCheckIntervalForTest(0.01);

        // Trigger pause - this sets up a 0.01s pressure watchdog timer
        $manager->check();
        $this->assertTrue($manager->isPaused());

        // Run the loop briefly to let the pressure timer fire.
        // Inflight count stays the same (30001 >= 30001) so shutdown should trigger.
        $loop = Loop::get();
        $loop->addTimer(0.1, $loop->stop(...));
        $loop->run();

        $this->assertTrue($shutdownCalled, 'Shutdown callback should be called when inflight actions do not reduce while paused');
    }

    public function testPressureReducingDoesNotTriggerShutdown(): void {
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        // Use a counter that decreases on every call so the watchdog always
        // sees progress between ticks (the original code triggers shutdown
        // only when the count does NOT strictly decrease between checks).
        $count = 30001;
        $loop = Loop::get();

        $this->manager = new class(
            $loop,
            function() use (&$count) { return $count--; },
            function() use (&$paused) { $paused = true; },
            function() use (&$resumed) { $resumed = true; },
            function() use (&$shutdownCalled) { $shutdownCalled = true; },
            50, 35, 30000, 500,
        ) extends MemoryManager {
            private int $fakeUsage = 0;
            public function setMemoryLimitForTest(int $limit): void { $this->memoryLimit = $limit; }
            public function setMemoryUsageForTest(int $usage): void { $this->fakeUsage = $usage; }
            public function setPressureCheckIntervalForTest(float $interval): void { $this->pressureCheckInterval = $interval; }
            protected function currentMemoryUsage(): int { return $this->fakeUsage; }
        };

        $this->manager->setMemoryLimitForTest(PHP_INT_MAX);
        $this->manager->setMemoryUsageForTest(0);
        $this->manager->setPressureCheckIntervalForTest(0.01);

        // Trigger pause
        $this->manager->check();
        $this->assertTrue($this->manager->isPaused());

        // Run the loop briefly - count keeps decreasing so watchdog sees progress
        $loop->addTimer(0.1, $loop->stop(...));
        $loop->run();

        $this->assertFalse($shutdownCalled, 'Shutdown should NOT be called when inflight actions keep reducing');
    }

    public function testStartAndStopManageTimers(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);
        $manager->setMemoryLimitForTest(1000);
        $manager->setMemoryUsageForTest(600);

        $manager->start();
        $manager->check(); // triggers pause + pressure timer
        $manager->stop();

        // After stop, timers should be cancelled - running the loop should not trigger anything
        $loop = Loop::get();
        $loop->addTimer(0.1, $loop->stop(...));
        $loop->run();

        $this->assertFalse($shutdownCalled, 'No shutdown after stop() cancelled timers');
    }

    public function testInitialiseDetectsMemoryLimit(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $logger = new TestLogger();
        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled, $logger);

        $manager->initialise();

        // Memory limit should be detected (non-zero or NO_LIMIT for unlimited)
        $this->assertNotSame(0, $manager->getMemoryLimit(), 'Memory limit should be detected after initialise()');

        $logger->assert()
            ->hasLog()
            ->withMessageContains('Memory limit set to');
    }

    public function testGettersReturnDefaultsBeforeAnyCheck(): void {
        $inflightCount = 0;
        $paused = false;
        $resumed = false;
        $shutdownCalled = false;

        $manager = $this->buildManager($inflightCount, $paused, $resumed, $shutdownCalled);

        $this->assertFalse($manager->isPaused());
        $this->assertSame(0, $manager->getPausedCount());
        $this->assertSame(0, $manager->getPercentUsed());
        $this->assertSame(0, $manager->getMemoryLimit());
    }
}

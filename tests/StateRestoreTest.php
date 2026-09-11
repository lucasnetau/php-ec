<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Scheduler\ObservableScheduler;
use EdgeTelemetrics\EventCorrelation\Tests\Rules\TimeoutRule;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\CorrelationEngine::setState
 * @covers \EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor::__unserialize
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler::setState
 */
class StateRestoreTest extends TestCase {

    public function testEngineSetStateRejectsMissingKeys() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $this->expectException(RuntimeException::class);
        $engine->setState(['matchers' => []]);
    }

    public function testEngineSetStateRejectsNonArraySections() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $this->expectException(RuntimeException::class);
        $engine->setState(['eventstream_live' => false, 'statistics' => 'nope', 'events' => [], 'matchers' => []]);
    }

    public function testEngineSetStateRejectsScalarEventEntry() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $this->expectException(RuntimeException::class);
        $engine->setState([
            'eventstream_live' => false,
            'statistics' => [],
            'events' => ['1' => 123],
            'matchers' => [],
        ]);
    }

    public function testEngineSetStateRejectsScalarMatcherEntry() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $this->expectException(RuntimeException::class);
        $engine->setState([
            'eventstream_live' => false,
            'statistics' => [],
            'events' => [],
            'matchers' => [123],
        ]);
    }

    public function testEngineSetStateDropsDamagedMatcher() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $logger = new TestLogger();
        $engine->setLogger($logger);

        $engine->setState([
            'eventstream_live' => false,
            'statistics' => [],
            'events' => [],
            'matchers' => [['c' => TimeoutRule::class, 's' => ['bogus' => true]]],
        ]);

        $this->assertCount(0, $engine->getState()['matchers'], 'damaged matcher must be dropped');
        $logger->assert()->hasWarning();
    }

    public function testEngineSetStateDropsMatcherWithMissingEventNameList() : void {
        $engine = new CorrelationEngine([TimeoutRule::class]);
        $logger = new TestLogger();
        $engine->setLogger($logger);

        $engine->setState([
            'eventstream_live' => false,
            'statistics' => [],
            'events' => [],
            'matchers' => [['c' => TimeoutRule::class, 's' => ['actionFired' => false, 'isTimedOut' => false, 'context' => []]]],
        ]);

        $this->assertCount(0, $engine->getState()['matchers']);
    }

    public function testMatcherInstanceIdSurvivesSerialisation() : void {
        $state = (new TimeoutRule())->jsonSerialize();

        $restored = new TimeoutRule();
        $restored->__unserialize($state);

        $this->assertSame($state['id'], $restored->jsonSerialize()['id']);
    }

    public function testMatcherAcceptsLegacyInstanceIdKey() : void {
        $state = (new TimeoutRule())->jsonSerialize();
        $legacyId = $state['id'];
        unset($state['id']);
        $state['instanceId'] = $legacyId;

        $restored = new TimeoutRule();
        $restored->__unserialize($state);

        $this->assertSame($legacyId, $restored->jsonSerialize()['id']);
    }

    public function testSchedulerRestoresStateMissingOptionalSections() : void {
        $file = tempnam(sys_get_temp_dir(), 'php-ec-restore-test-');
        file_put_contents($file, json_encode([
            'engine' => ['eventstream_live' => false, 'statistics' => [], 'events' => [], 'matchers' => []],
            'scheduler' => ['state' => 'unclean shutdown'],
        ]));

        try {
            $scheduler = new ObservableScheduler([]);
            $scheduler->setLogger(new TestLogger());
            $scheduler->setSavefileName($file);
            $scheduler->setSaveStateInterval(600);

            $reachedRunning = false;
            $scheduler->on('running', function () use ($scheduler, &$reachedRunning) {
                $reachedRunning = true;
                $scheduler->shutdown();
            });

            $scheduler->run();

            $this->assertTrue($reachedRunning, 'scheduler must boot from a state file missing optional sections');
        } finally {
            @unlink($file);
        }
    }
}

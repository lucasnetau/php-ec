<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\Tests\Rules\TimeoutRule;
use PHPUnit\Framework\TestCase;
use function array_shift;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\CorrelationEngine
 */
class EngineTest extends TestCase {

    public function testEngineThrowsOnUndefinedRuleClass(): void {
        $this->expectException(\RuntimeException::class);
        new CorrelationEngine(['NonExistentRuleClass']);
    }

    public function testEngineThrowsOnDuplicateRuleClassString(): void {
        $this->expectException(\RuntimeException::class);
        new CorrelationEngine([[TimeoutRule::class, []],[TimeoutRule::class, []]]);
    }

    public function testEngineCanSerialiseStateWithClassTypeRule() : void {
        $engine = new CorrelationEngine([TimeoutRule::class,]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE]));

        $state = $engine->getState();
        $this->assertNotNull($state);
    }

    public function testEngineCanSerialiseAndRestoreStateWithClassConstructorArrayTypeRule() : void {
        $engine = new CorrelationEngine([[TimeoutRule::class,[]]]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE]));

        $state = $engine->getState();
        $this->assertIsArray($state);
        $this->assertCount(1, $state['matchers']);

        $engine2 = new CorrelationEngine([[TimeoutRule::class,[]]]);
        $engine->setState($state);
        $this->assertIsArray($state);
        $this->assertCount(1, $state['matchers']);
    }

    public function testEngineCanSerialiseStateWithObjectInstanceTypeRule() : void {
        $rule = new TimeoutRule();
        $engine = new CorrelationEngine([$rule]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE]));

        $state = $engine->getState();
        $this->assertIsArray($state);
        $this->assertCount(1, $state['matchers']);
    }

    public function testEngineSkipsSerialisingAnonymousClassTypeRule() : void {
        /** One-shot Anonymous class */
        $rule = new class() extends \EdgeTelemetrics\EventCorrelation\Rule\MatchSingleContinuously {
            const EVENTS = [[self::EVENT_MATCH_ANY]];
            const TIMEOUT = 'PT15S';
        };

        $engine = new CorrelationEngine([$rule,]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE]));

        $state = $engine->getState();
        $this->assertIsArray($state);
        $this->assertCount(0, $state['matchers']);
    }

    public function testEngineSortsTimeoutsEarliestToLatestTimeout() : void {
        $timer1 = new class() extends \EdgeTelemetrics\EventCorrelation\Rule\MatchSingleContinuously {

            const EVENTS = [[self::EVENT_MATCH_ANY]];
            const TIMEOUT = 'PT15S';
        };

        $timer2 = new class() extends \EdgeTelemetrics\EventCorrelation\Rule\MatchSingleContinuously {

            const EVENTS = [[self::EVENT_MATCH_ANY]];
            const TIMEOUT = 'PT50S';
        };

        $engine = new CorrelationEngine([$timer1, $timer2,]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_NEW_STATE]));

        $timeouts = $engine->getTimeouts();
        $this->assertCount(2, $timeouts);
        $first = array_shift($timeouts);
        $second = array_shift($timeouts);
        $this->assertTrue($first <= $second);
    }
}
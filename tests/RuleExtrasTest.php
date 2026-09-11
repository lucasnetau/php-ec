<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\CorrelationEngine;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule\OnHeartbeat;
use EdgeTelemetrics\EventCorrelation\Rule\ValidateEnrichFilter;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use EdgeTelemetrics\EventCorrelation\StateMachine\AEventProcessor;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Rule\ValidateEnrichFilter
 * @covers \EdgeTelemetrics\EventCorrelation\Rule\OnHeartbeat
 */
class RuleExtrasTest extends TestCase {

    private function buildFilterRule() : ValidateEnrichFilter {
        return new class extends ValidateEnrichFilter {
            const EVENTS = [['Test:Input']];

            public int $passed = 0;
            public int $failed = 0;
            public ?Event $captured = null;

            protected function validate(array $event) : ?array {
                return ($event['value'] ?? 0) > 0 ? $event : null;
            }

            protected function enrich(?array $event) : ?array {
                if ($event !== null) {
                    $event['enriched'] = true;
                }
                return $event;
            }

            protected function filter(?array $event) : ?array {
                return (($event['enriched'] ?? false) && $event['value'] > 10) ? $event : null;
            }

            protected function firePassed() : void {
                $this->passed++;
                $this->captured = $this->finalEvent;
            }

            protected function fireFailed() : void {
                $this->failed++;
            }
        };
    }

    public function testValidateEnrichFilterPassesEnrichedEvent() : void {
        $rule = $this->buildFilterRule();

        $result = $rule->handle(new Event(['event' => 'Test:Input', 'value' => 42]));
        $rule->fire();

        $this->assertSame(AEventProcessor::EVENT_HANDLED, $result);
        $this->assertSame(1, $rule->passed);
        $this->assertSame(0, $rule->failed);
        $this->assertInstanceOf(Event::class, $rule->captured);
        $this->assertTrue($rule->captured->enriched);
        $this->assertSame(42, $rule->captured->value);
    }

    public function testValidateEnrichFilterSuppressesWhenFilterRejects() : void {
        $rule = $this->buildFilterRule();

        $result = $rule->handle(new Event(['event' => 'Test:Input', 'value' => 5]));
        $rule->fire();

        $this->assertSame(AEventProcessor::EVENT_HANDLED | AEventProcessor::EVENT_SUPPRESS, $result);
        $this->assertSame(0, $rule->passed);
        $this->assertSame(1, $rule->failed);
    }

    public function testValidateEnrichFilterSuppressesWhenValidationFails() : void {
        $rule = $this->buildFilterRule();

        $result = $rule->handle(new Event(['event' => 'Test:Input', 'value' => -1]));
        $rule->fire();

        $this->assertSame(AEventProcessor::EVENT_HANDLED | AEventProcessor::EVENT_SUPPRESS, $result);
        $this->assertSame(1, $rule->failed);
    }

    public function testOnHeartbeatCompletesOnHeartbeatEvent() : void {
        $rule = new class extends OnHeartbeat {
            public static int $completed = 0;

            public function onComplete() : void {
                self::$completed++;
            }
        };

        $engine = new CorrelationEngine([$rule]);
        $engine->handle(new Event(['event' => Scheduler::CONTROL_MSG_HEARTBEAT, 'runtime' => 5, 'seq' => 1]));

        $this->assertSame(1, $rule::$completed);
    }
}

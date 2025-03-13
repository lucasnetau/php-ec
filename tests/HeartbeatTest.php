<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Scheduler\Heartbeat;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use function abs;
use function count;
use function in_array;
use function max;
use function min;
use function round;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Scheduler\Heartbeat
 */
class HeartbeatTest extends TestCase {

    public function testHeartbeatPulseOnScheduleWithoutDelay(): void {
        Loop::addTimer(0.4, Loop::stop(...));

        $heartbeat = new Heartbeat(0.1);

        $heartbeats = [];
        $heartbeat->on('pulse', function($runtime, $seq) use (&$heartbeats) {
           $heartbeats[] = $runtime;
        });

        $heartbeat->start(Loop::get(), 0);

        Loop::get()->run();

        $this->assertNotEmpty($heartbeats, 'No Heartbeats received');
        $this->assertThat(count($heartbeats), $this->logicalAnd(
            $this->greaterThanOrEqual(4),
            $this->lessThanOrEqual(5)
        ), 'Unexpected number of heartbeats');

        $diffs = array();
        for ($i = 1, $n = count($heartbeats); $i < $n; $i++) {
            $diffs[] = ($heartbeats[$i] - $heartbeats[$i-1])/1e+3; //Convert micro to milli seconds
        }
        $min = round(min($diffs));
        $max = round(max($diffs));
        $expected = 0.1 * 1e+3;

        $this->assertTrue(abs($expected - $min) < 10, 'Heartbeat is too quick');
        $this->assertTrue(abs($expected - $max) < 10, 'Heartbeat is too slow');
    }

    public function testHeartbeatPulseOnScheduleWithDelay(): void {
        Loop::addTimer(0.6, Loop::stop(...));

        $intSeconds = 0.1;
        $heartbeat = new Heartbeat($intSeconds);

        $heartbeats = [];
        $heartbeat->on('pulse', function($runtime, $seq) use (&$heartbeats) {
            $heartbeats[] = $runtime;
        });

        $delay = 0.3;
        $heartbeat->start(Loop::get(), $delay);

        Loop::get()->run();

        $this->assertNotEmpty($heartbeats, 'No Heartbeats received');
        $this->assertThat(count($heartbeats), $this->logicalAnd(
            $this->greaterThanOrEqual(3),
            $this->lessThanOrEqual(4)
        ), 'Unexpected number of heartbeats');
        $this->assertTrue(abs($heartbeats[0]/1e+6)-($delay*1e+3) < 10, 'Heartbeat started before initial delay');

        $diffs = array();
        for ($i = 1, $n = count($heartbeats); $i < $n; $i++) {
            $diffs[] = ($heartbeats[$i] - $heartbeats[$i-1])/1e+3; //Convert micro to milli seconds
        }
        $min = round(min($diffs));
        $max = round(max($diffs));
        $expected = $intSeconds * 1e+3;

        $this->assertTrue(abs($expected - $min) < 10, 'Heartbeat is too quick');
        $this->assertTrue(abs($expected - $max) < 10, 'Heartbeat is too slow');
    }
}
<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\SaveHandler\FileAdapter;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;
use Throwable;
use VStelmakh\PsrTestLogger\TestLogger;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * Seeded property/fuzz tests. The PRNG is seeded in setUp() so failures are reproducible.
 * These tests assert the library never fatals or loses data on adversarial input; they do
 * not try to enumerate valid inputs.
 */
class FuzzTest extends TestCase {

    protected function setUp(): void {
        mt_srand(0xF0F0F0F0);
        parent::setUp();
    }

    private function randomJsonValue(int $depth = 0): mixed {
        $max = $depth >= 2 ? 5 : 7;
        switch (mt_rand(0, $max)) {
            case 0: return null;
            case 1: return (bool)mt_rand(0, 1);
            case 2: return mt_rand(-1000000, 1000000);
            case 3: return mt_rand(-1000, 1000) / 8; // binary fraction, exact in JSON
            case 4: return '';
            case 5: return 'str:' . mt_rand();
            case 6: return 'unicode:δπ🎲' . mt_rand();
            default:
                $array = [];
                $count = mt_rand(0, 3);
                for ($i = 0; $i < $count; $i++) {
                    $array['k' . mt_rand(0, 9)] = $this->randomJsonValue($depth + 1);
                }
                return $array;
        }
    }

    private function randomEventPayload(): array {
        $kvp = [];
        foreach (['id', 'value', 'sensor', 'reading'] as $key) {
            if (mt_rand(0, 1) === 1) {
                $kvp[$key] = $this->randomJsonValue();
            }
        }

        switch (mt_rand(0, 9)) {
            case 0: break; // missing event name
            case 1: $kvp['event'] = ['not', 'a', 'string']; break;
            case 2: $kvp['event'] = mt_rand(); break;
            case 3: $kvp['event'] = ''; break;
            default: $kvp['event'] = 'Fuzz:' . mt_rand(0, 99999); break;
        }

        switch (mt_rand(0, 9)) {
            case 0: break; // server time
            case 1: $kvp['datetime'] = mt_rand(-1_000_000_000, 2_000_000_000); break;
            case 2: $kvp['datetime'] = 'not-a-date'; break;
            case 3: $kvp['datetime'] = (string)mt_rand(0, 2_000_000_000); break;
            case 4: $kvp['datetime'] = new DateTimeImmutable('@' . mt_rand(0, 2_000_000_000)); break;
            default: $kvp['datetime'] = gmdate('Y-m-d\TH:i:s\Z', mt_rand(0, 2_000_000_000)); break;
        }

        if (mt_rand(0, 9) === 0) {
            $kvp['receivedTime'] = (mt_rand(0, 1) === 1) ? new DateTimeImmutable() : mt_rand();
        }

        return $kvp;
    }

    public function testEventStateRoundTripFuzz() : void {
        $checked = 0;
        for ($i = 0; $i < 300; $i++) {
            try {
                $event = new Event($this->randomEventPayload());
            } catch (Throwable $ex) {
                continue; // invalid payloads are the ingestion fuzz test's job
            }

            try {
                $name = $event->getEventName();
            } catch (Throwable $ex) {
                continue; // no event name; only the RPC boundary needs to reject these
            }
            if ($name === '') {
                continue;
            }

            $checked++;
            $state = $event->toArray();
            $this->assertIsString(json_encode($state), "event state must be JSON encodable at iteration $i");

            $restored = (new ReflectionClass(Event::class))->newInstanceWithoutConstructor();
            $restored->__unserialize($state);

            $this->assertSame($event->getEventName(), $restored->getEventName(), "name must round-trip at iteration $i");
            $this->assertSame($event->datetime->getTimestamp(), $restored->datetime->getTimestamp(), "datetime must round-trip at iteration $i");
            $this->assertSame(
                json_encode($state),
                json_encode($restored->toArray()),
                "event state must round-trip at iteration $i"
            );
        }

        $this->assertGreaterThan(100, $checked, 'fuzz corpus should produce enough valid events');
    }

    private function randomStateArray(int $depth = 0): array {
        $state = [];
        $count = mt_rand(0, 4);
        for ($i = 0; $i < $count; $i++) {
            $key = 'k' . mt_rand(0, 9);
            $state[$key] = ($depth < 2 && mt_rand(0, 3) === 0)
                ? $this->randomStateArray($depth + 1)
                : $this->randomJsonValue();
        }
        return $state;
    }

    public function testSaveStateRoundTripFuzz() : void {
        $file = tempnam(sys_get_temp_dir(), 'php-ec-fuzz-state-');
        @unlink($file);
        $adapter = new FileAdapter($file, new TestLogger(), null);

        try {
            for ($i = 0; $i < 150; $i++) {
                $state = $this->randomStateArray();
                $adapter->saveStateSync($state);
                $this->assertFileExists($file, "save must write a file at iteration $i");
                $this->assertSame($state, $adapter->loadState(), "state must round-trip at iteration $i");
            }
        } finally {
            @unlink($file);
        }
    }

    private function mutateSaveFile(string $contents): string {
        switch (mt_rand(0, 8)) {
            case 0:
                return '';
            case 1:
                return 'null';
            case 2:
                return (string)mt_rand(-1000, 1000);
            case 3:
                return 'true';
            case 4:
                return gzencode((string)mt_rand(-1000, 1000), 2) ?: '';
            case 5:
                return gzencode('"a string"', 2) ?: '';
            case 6:
                return substr($contents, 0, mt_rand(0, strlen($contents) - 1));
            case 7:
                $pos = mt_rand(0, strlen($contents) - 1);
                $contents[$pos] = chr(mt_rand(0, 255));
                return $contents;
            default:
                $pos = mt_rand(0, strlen($contents));
                $insert = '';
                for ($i = 0, $n = mt_rand(1, 8); $i < $n; $i++) {
                    $insert .= chr(mt_rand(0, 255));
                }
                return substr($contents, 0, $pos) . $insert . substr($contents, $pos);
        }
    }

    public function testLoadStateSurvivesCorruptedSaveFilesFuzz() : void {
        $file = tempnam(sys_get_temp_dir(), 'php-ec-fuzz-corrupt-');
        $adapter = new FileAdapter($file, new TestLogger(), null);
        $adapter->saveStateSync(['scheduler' => ['state' => 'RUNNING'], 'engine' => [], 'memory' => []]);
        $valid = file_get_contents($file);

        try {
            for ($i = 0; $i < 250; $i++) {
                file_put_contents($file, $this->mutateSaveFile($valid));
                try {
                    $result = $adapter->loadState();
                } catch (RuntimeException $ex) {
                    continue; // corrupted JSON is reported via RuntimeException per contract
                }
                $this->assertTrue(
                    $result === false || is_array($result),
                    'loadState must return array|false, got ' . gettype($result) . " at iteration $i"
                );
            }
        } finally {
            @unlink($file);
        }
    }
}

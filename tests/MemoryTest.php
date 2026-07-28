<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use DateTimeImmutable;
use EdgeTelemetrics\EventCorrelation\Memory\ArrayMemory;
use EdgeTelemetrics\EventCorrelation\Memory\MemoryEntry;
use EdgeTelemetrics\EventCorrelation\Memory\MemoryEngine;
use EdgeTelemetrics\EventCorrelation\Memory\MemoryWrite;
use EdgeTelemetrics\EventCorrelation\Memory\JsonFileBackend;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

class MemoryTest extends TestCase
{
    public function testArrayMemorySetAndGet(): void
    {
        $mem = new ArrayMemory();
        $entry = new MemoryEntry('sensor/42', 'compressor', true);
        $mem->set($entry);

        $this->assertTrue($mem->has('sensor/42', 'compressor'));
        $this->assertTrue($mem->get('sensor/42', 'compressor'));
    }

    public function testArrayMemoryGetMissing(): void
    {
        $mem = new ArrayMemory();
        $this->assertNull($mem->get('missing', 'key'));
        $this->assertFalse($mem->has('missing', 'key'));
    }

    public function testArrayMemoryAll(): void
    {
        $mem = new ArrayMemory();
        $mem->set(new MemoryEntry('sensor/42', 'compressor', true));
        $mem->set(new MemoryEntry('sensor/42', 'mode', 'cooling'));
        $mem->set(new MemoryEntry('sensor/99', 'compressor', false));

        $entries = iterator_to_array($mem->all('sensor/42'));
        $this->assertCount(2, $entries);
        $this->assertTrue($entries['compressor']);
        $this->assertSame('cooling', $entries['mode']);
    }

    public function testArrayMemoryDelete(): void
    {
        $mem = new ArrayMemory();
        $mem->set(new MemoryEntry('sensor/42', 'compressor', true));
        $this->assertTrue($mem->has('sensor/42', 'compressor'));

        $mem->delete('sensor/42', 'compressor');
        $this->assertFalse($mem->has('sensor/42', 'compressor'));
    }

    public function testArrayMemoryExpiredEntry(): void
    {
        $mem = new ArrayMemory();
        $expiresAt = new DateTimeImmutable('-1 second');
        $entry = new MemoryEntry('sensor/42', 'compressor', true, $expiresAt);
        $mem->set($entry);

        $this->assertFalse($mem->has('sensor/42', 'compressor'));
        $this->assertNull($mem->get('sensor/42', 'compressor'));
    }

    public function testArrayMemoryPurgeExpired(): void
    {
        $mem = new ArrayMemory();
        $mem->set(new MemoryEntry('sensor/42', 'expired', true, new DateTimeImmutable('-1 second')));
        $mem->set(new MemoryEntry('sensor/42', 'valid', true));

        $expired = $mem->purgeExpired();
        $this->assertSame(1, $expired);
        $this->assertFalse($mem->has('sensor/42', 'expired'));
        $this->assertTrue($mem->has('sensor/42', 'valid'));
    }

    public function testArrayMemoryPopulate(): void
    {
        $mem = new ArrayMemory();
        $entries = [
            new MemoryEntry('sensor/42', 'compressor', true),
            new MemoryEntry('sensor/42', 'mode', 'cooling'),
        ];
        $mem->populate($entries);

        $this->assertCount(2, iterator_to_array($mem->all('sensor/42')));
    }

    public function testArrayMemoryClear(): void
    {
        $mem = new ArrayMemory();
        $mem->set(new MemoryEntry('sensor/42', 'compressor', true));
        $mem->clear();
        $this->assertFalse($mem->has('sensor/42', 'compressor'));
    }

    public function testMemoryEngineApplyWrite(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrite(new MemoryWrite('sensor/42/state', 'compressor', true));

        $this->assertTrue($engine->has('sensor/42/state', 'compressor'));
        $this->assertTrue($engine->get('sensor/42/state', 'compressor'));
    }

    public function testMemoryEngineApplyWrites(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrites([
            new MemoryWrite('sensor/42/state', 'compressor', true),
            new MemoryWrite('sensor/42/state', 'mode', 'cooling'),
        ]);

        $this->assertCount(2, iterator_to_array($engine->all('sensor/42/state')));
    }

    public function testMemoryEngineApplyWriteWithTtl(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrite(new MemoryWrite('weather', 'temp', 25, ttl: 300));

        $this->assertTrue($engine->has('weather', 'temp'));
        $this->assertSame(25, $engine->get('weather', 'temp'));
    }

    public function testMemoryEnginePurgeExpired(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrite(new MemoryWrite('weather', 'expired', 25, ttl: -1));

        $expired = $engine->purgeExpired();
        $this->assertGreaterThanOrEqual(1, $expired);
    }

    public function testMemoryEngineState(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrites([
            new MemoryWrite('sensor/42', 'compressor', true),
            new MemoryWrite('sensor/42', 'mode', 'cooling'),
        ]);

        $state = $engine->getState();
        $this->assertCount(2, $state);

        $engine2 = new MemoryEngine();
        $engine2->setState($state);

        $this->assertTrue($engine2->has('sensor/42', 'compressor'));
        $this->assertSame('cooling', $engine2->get('sensor/42', 'mode'));
    }

    public function testMemoryEngineClear(): void
    {
        $engine = new MemoryEngine();
        $engine->applyWrite(new MemoryWrite('sensor/42', 'compressor', true));
        $engine->clear();
        $this->assertFalse($engine->has('sensor/42', 'compressor'));
    }

    public function testJsonFileBackendRoundTrip(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'memtest_') . '.json';
        try {
            $backend = new JsonFileBackend($tmpFile);
            $entries = [
                new MemoryEntry('sensor/42', 'compressor', true, null, true),
                new MemoryEntry('sensor/42', 'mode', 'cooling', null, true),
            ];

            $backend->save($entries);
            $loaded = $backend->load();

            $this->assertCount(2, $loaded);
            $this->assertSame('sensor/42', $loaded[0]->namespace);
            $this->assertSame('compressor', $loaded[0]->key);
            $this->assertTrue($loaded[0]->value);
            $this->assertTrue($loaded[0]->persistent);
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testJsonFileBackendLoadEmpty(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'memtest_') . '.json';
        try {
            $backend = new JsonFileBackend($tmpFile);
            $loaded = $backend->load();
            $this->assertCount(0, $loaded);
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testJsonFileBackendLoadCorruptedReturnsEmpty(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'memtest_') . '.json';
        try {
            file_put_contents($tmpFile, '{not valid json[[[');
            $backend = new JsonFileBackend($tmpFile);
            $loaded = $backend->load();
            $this->assertCount(0, $loaded);
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testJsonFileBackendCompressedRoundTrip(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'memtest_') . '.json';
        try {
            $backend = new JsonFileBackend($tmpFile);
            $entries = [
                new MemoryEntry('sensor/42', 'compressor', true, null, true),
                new MemoryEntry('sensor/42', 'mode', 'cooling', null, true),
            ];

            $backend->save($entries);

            // Verify file is gzip-compressed
            $raw = file_get_contents($tmpFile);
            $this->assertStringStartsWith("\x1f\x8b", $raw, 'File should be gzip-compressed');

            // Verify load still works
            $loaded = $backend->load();
            $this->assertCount(2, $loaded);
            $this->assertTrue($loaded[0]->value);
            $this->assertSame('cooling', $loaded[1]->value);
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testMemoryEngineWithBackend(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'memtest_') . '.json';
        try {
            $backend = new JsonFileBackend($tmpFile);
            $engine = new MemoryEngine($backend);

            // Write persistent entry
            $engine->applyWrite(new MemoryWrite('sensor/42', 'type', 'freezer', persistent: true));
            $engine->persist();

            // Create new engine and load from backend
            $engine2 = new MemoryEngine($backend);
            $engine2->loadFromBackend();

            $this->assertTrue($engine2->has('sensor/42', 'type'));
            $this->assertSame('freezer', $engine2->get('sensor/42', 'type'));
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testMemoryWriteJsonSerialize(): void
    {
        $write = new MemoryWrite('sensor/42/state', 'compressor', true, ttl: 300, persistent: true);
        $json = json_encode($write);
        $data = json_decode($json, true);

        $this->assertSame('sensor/42/state', $data['namespace']);
        $this->assertSame('compressor', $data['key']);
        $this->assertTrue($data['value']);
        $this->assertSame(300, $data['ttl']);
        $this->assertTrue($data['persistent']);
    }

    public function testMemoryEntryJsonSerialize(): void
    {
        $entry = new MemoryEntry('sensor/42', 'compressor', true, null, true);
        $json = json_encode($entry);
        $data = json_decode($json, true);

        $this->assertSame('sensor/42', $data['namespace']);
        $this->assertSame('compressor', $data['key']);
        $this->assertTrue($data['value']);
        $this->assertNull($data['expiresAt']);
        $this->assertTrue($data['persistent']);
    }

    public function testMemoryEntryFromArray(): void
    {
        $data = [
            'namespace' => 'sensor/42',
            'key' => 'compressor',
            'value' => true,
            'expiresAt' => '2030-01-01T00:00:00+00:00',
            'persistent' => true,
        ];
        $entry = MemoryEntry::fromArray($data);

        $this->assertSame('sensor/42', $entry->namespace);
        $this->assertSame('compressor', $entry->key);
        $this->assertTrue($entry->value);
        $this->assertInstanceOf(DateTimeImmutable::class, $entry->expiresAt);
        $this->assertTrue($entry->persistent);
    }

    public function testPreloadScriptReturnsWritesAppliedToEngine(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'mempreload_') . '.php';
        try {
            file_put_contents($tmpFile, '<?php return ['
                . 'new \EdgeTelemetrics\EventCorrelation\Memory\MemoryWrite("sensor/42/config", "type", "freezer"),'
                . 'new \EdgeTelemetrics\EventCorrelation\Memory\MemoryWrite("site/config", "timezone", "Australia/Sydney"),'
            . '];');

            $engine = new MemoryEngine();
            $writes = require $tmpFile;

            $this->assertIsArray($writes);
            $this->assertCount(2, $writes);

            $engine->applyWrites($writes);

            $this->assertSame('freezer', $engine->get('sensor/42/config', 'type'));
            $this->assertSame('Australia/Sydney', $engine->get('site/config', 'timezone'));
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testPreloadScriptReturnsEmptyArray(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'mempreload_') . '.php';
        try {
            file_put_contents($tmpFile, '<?php return [];');

            $engine = new MemoryEngine();
            $writes = require $tmpFile;

            $this->assertIsArray($writes);
            $this->assertCount(0, $writes);

            $engine->applyWrites($writes);
            $this->assertFalse($engine->has('any', 'key'));
        } finally {
            @unlink($tmpFile);
        }
    }

    public function testPreloadScriptInvalidReturnThrows(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'mempreload_') . '.php';
        try {
            file_put_contents($tmpFile, '<?php return "not an array";');

            $writes = require $tmpFile;
            $this->assertIsString($writes);
            $this->assertIsNotArray($writes);
        } finally {
            @unlink($tmpFile);
        }
    }
}

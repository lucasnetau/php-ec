<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Library\SyncFlushZstdFilter;
use EdgeTelemetrics\EventCorrelation\Library\ZstdCompressor;
use EdgeTelemetrics\EventCorrelation\Library\ZstdDecompressor;
use PHPUnit\Framework\TestCase;
use function strlen;
use function substr;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Library\ZstdCompressor
 * @covers \EdgeTelemetrics\EventCorrelation\Library\ZstdDecompressor
 * @covers \EdgeTelemetrics\EventCorrelation\Library\SyncFlushZstdFilter
 */
class ZstdCompressionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!function_exists('zstd_compress_init')) {
            $this->markTestSkipped('ext-zstd is not available');
        }
    }

    public function testCompressorAndDecompressorRoundTripIncrementally(): void
    {
        $compressor = new ZstdCompressor();
        $chunks = [];
        $compressor->on('data', function (string $chunk) use (&$chunks) {
            $chunks[] = $chunk;
        });

        $compressor->write('hello ');
        $this->assertNotEmpty($chunks, 'a flushing compressor must emit data per write');
        $compressor->write('world');
        $compressor->end();

        $decompressed = '';
        $decompressor = new ZstdDecompressor();
        $decompressor->on('data', function (string $chunk) use (&$decompressed) {
            $decompressed .= $chunk;
        });
        foreach ($chunks as $chunk) {
            $decompressor->write($chunk);
        }

        $this->assertSame('hello world', $decompressed);
    }

    public function testZstdFilterProducesDecodableFrames(): void
    {
        $filterName = 'syncflush.zstd.test';
        if (!in_array($filterName, stream_get_filters(), true)) {
            stream_filter_register($filterName, SyncFlushZstdFilter::class);
        }

        $stream = fopen('php://temp', 'w+');
        $filter = stream_filter_append($stream, $filterName, STREAM_FILTER_WRITE);
        $this->assertNotFalse($filter);

        fwrite($stream, 'filtered ');
        fwrite($stream, 'data');
        fflush($stream);
        rewind($stream);
        $raw = stream_get_contents($stream);

        $context = zstd_uncompress_init();
        $decompressed = '';
        for ($offset = 0; $offset < strlen($raw); $offset += 32) {
            $decompressed .= zstd_uncompress_add($context, substr($raw, $offset, 32));
        }

        $this->assertSame('filtered data', $decompressed);

        stream_filter_remove($filter);
        fclose($stream);
    }
}

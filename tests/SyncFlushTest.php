<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Library\SyncFlushCompressor;
use EdgeTelemetrics\EventCorrelation\Library\SyncFlushDeflateFilter;
use PHPUnit\Framework\TestCase;
use function strlen;
use function substr;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\Library\SyncFlushCompressor
 * @covers \EdgeTelemetrics\EventCorrelation\Library\SyncFlushDeflateFilter
 */
class SyncFlushTest extends TestCase {

    public function testCompressorEmitsFlushedFramesThatInflateIncrementally() : void {
        $compressor = new SyncFlushCompressor(ZLIB_ENCODING_RAW);
        $chunks = [];
        $compressor->on('data', function (string $chunk) use (&$chunks) {
            $chunks[] = $chunk;
        });

        $compressor->write('hello ');
        $this->assertNotEmpty($chunks, 'a sync-flushed compressor must emit data per write');
        $compressor->write('world');
        $compressor->end();

        $context = inflate_init(ZLIB_ENCODING_RAW);
        $decompressed = '';
        foreach ($chunks as $chunk) {
            $decompressed .= inflate_add($context, $chunk, ZLIB_SYNC_FLUSH);
        }

        $this->assertSame('hello world', $decompressed);
    }

    public function testDeflateFilterProducesInflatableFrames() : void {
        $filterName = 'syncflush.deflate.test';
        if (!in_array($filterName, stream_get_filters(), true)) {
            stream_filter_register($filterName, SyncFlushDeflateFilter::class);
        }

        $stream = fopen('php://temp', 'w+');
        $filter = stream_filter_append($stream, $filterName, STREAM_FILTER_WRITE);
        $this->assertNotFalse($filter);

        fwrite($stream, 'filtered ');
        fwrite($stream, 'data');
        fflush($stream);
        rewind($stream);
        $raw = stream_get_contents($stream);

        $context = inflate_init(ZLIB_ENCODING_RAW);
        $decompressed = '';
        for ($offset = 0; $offset < strlen($raw); $offset += 32) {
            $decompressed .= inflate_add($context, substr($raw, $offset, 32), ZLIB_SYNC_FLUSH);
        }

        $this->assertSame('filtered data', $decompressed);

        stream_filter_remove($filter);
        fclose($stream);
    }
}

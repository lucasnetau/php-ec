<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

use Clue\React\Zlib\TransformStream;
use InvalidArgumentException;

use function EdgeTelemetrics\EventCorrelation\rpcCompressionLevel;
use function zstd_compress_add;
use function zstd_compress_init;

/**
 * Zstd compressor usable in a React stream pipeline. Each write is flushed as
 * its own output chunk (zstd_compress_add uses ZSTD_e_flush), matching the
 * SyncFlushCompressor behaviour used for raw deflate.
 */
final class ZstdCompressor extends TransformStream
{
    private $context;

    public function __construct(?int $level = null)
    {
        $context = zstd_compress_init($level ?? rpcCompressionLevel());
        if ($context === false) {
            throw new InvalidArgumentException('Unable to initialize zstd compressor');
        }
        $this->context = $context;
    }

    protected function transformData($data): void
    {
        $ret = zstd_compress_add($this->context, $data);
        if ($ret !== '' && $ret !== false) {
            $this->emit('data', [$ret]);
        }
    }

    protected function transformEnd($data): void
    {
        $ret = zstd_compress_add($this->context, $data, true);
        $this->context = null;
        if ($ret !== '' && $ret !== false) {
            $this->emit('data', [$ret]);
        }
        $this->emit('end');
        $this->close();
    }
}

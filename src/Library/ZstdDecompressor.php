<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

use Clue\React\Zlib\TransformStream;
use InvalidArgumentException;
use RuntimeException;

use function zstd_uncompress_add;
use function zstd_uncompress_init;

/**
 * Zstd decompressor usable in a React stream pipeline.
 */
final class ZstdDecompressor extends TransformStream
{
    private $context;

    public function __construct()
    {
        $context = zstd_uncompress_init();
        if ($context === false) {
            throw new InvalidArgumentException('Unable to initialize zstd decompressor');
        }
        $this->context = $context;
    }

    protected function transformData($data): void
    {
        $ret = zstd_uncompress_add($this->context, $data);
        if ($ret === false) {
            throw new RuntimeException('Unable to decompress zstd data');
        }
        if ($ret !== '') {
            $this->emit('data', [$ret]);
        }
    }

    protected function transformEnd($data): void
    {
        if ($data !== '') {
            $this->transformData($data);
        }
        $this->context = null;
        $this->emit('end');
        $this->close();
    }
}

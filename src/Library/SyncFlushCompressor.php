<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

use Clue\React\Zlib\TransformStream;

use function deflate_add;
use function deflate_init;

final class SyncFlushCompressor extends TransformStream
{
    private $context;

    public function __construct(int $encoding, int $level = -1)
    {
        $this->context = deflate_init($encoding, ['level' => $level]);
    }

    protected function transformData($data): void
    {
        $ret = deflate_add($this->context, $data, ZLIB_SYNC_FLUSH);
        if ($ret !== '') {
            $this->emit('data', [$ret]);
        }
    }

    protected function transformEnd($data): void
    {
        $ret = deflate_add($this->context, $data, ZLIB_FINISH);
        $this->context = null;
        if ($ret !== '') {
            $this->emit('data', [$ret]);
        }
        $this->emit('end');
        $this->close();
    }
}

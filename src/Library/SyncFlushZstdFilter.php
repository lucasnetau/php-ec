<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

use php_user_filter;

use function EdgeTelemetrics\EventCorrelation\rpcCompressionLevel;
use function strlen;
use function stream_bucket_append;
use function stream_bucket_make_writeable;
use function zstd_compress_add;
use function zstd_compress_init;

/**
 * STDOUT stream filter applying zstd with a flush per bucket, for source processes.
 */
class SyncFlushZstdFilter extends php_user_filter
{
    private $context;

    public function onCreate(): bool
    {
        $this->context = zstd_compress_init(rpcCompressionLevel());
        return $this->context !== false;
    }

    public function filter($in, $out, &$consumed, $closing): int
    {
        if ($this->context === null) {
            return PSFS_ERR_FATAL;
        }
        while ($bucket = stream_bucket_make_writeable($in)) {
            $consumed += (int)$bucket->datalen;
            $compressed = zstd_compress_add($this->context, $bucket->data, (bool)$closing);
            if ($compressed === false) {
                return PSFS_ERR_FATAL;
            }
            $bucket->data = $compressed;
            $bucket->datalen = strlen($compressed);
            stream_bucket_append($out, $bucket);
        }
        return PSFS_PASS_ON;
    }
}

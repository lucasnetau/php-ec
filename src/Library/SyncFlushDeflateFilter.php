<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library;

class SyncFlushDeflateFilter extends \php_user_filter
{
    private $context;

    public function onCreate(): bool
    {
        $this->context = deflate_init(ZLIB_ENCODING_RAW, ['level' => -1]);
        return true;
    }

    public function filter($in, $out, &$consumed, $closing): int
    {
        if ($this->context === null) {
            return PSFS_ERR_FATAL;
        }
        while ($bucket = stream_bucket_make_writeable($in)) {
            $consumed += $bucket->datalen;
            $bucket->data = deflate_add($this->context, $bucket->data, $closing ? ZLIB_FINISH : ZLIB_SYNC_FLUSH);
            $bucket->datalen = \strlen($bucket->data);
            stream_bucket_append($out, $bucket);
        }
        return PSFS_PASS_ON;
    }
}
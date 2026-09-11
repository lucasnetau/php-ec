<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests\Helpers;

use php_user_filter;

class RedirectStreamFilter extends php_user_filter {
    function filter($in, $out, &$consumed, $closing) : int {
        while ($bucket = stream_bucket_make_writeable($in)) {
            $consumed += $bucket->datalen;
            fwrite($this->params['redirect'], $bucket->data);
        }
        return PSFS_PASS_ON;
    }
}

stream_filter_register("redirect", RedirectStreamFilter::class);
<?php declare(strict_types=1);

/**
 * Stubs for the optional ext-zstd (kjdev/php-ext-zstd), used by static analysis.
 * Not loaded at runtime; the real functions/classes come from the extension.
 */

namespace {
    const ZSTD_COMPRESS_LEVEL_MIN = -131072;
    const ZSTD_COMPRESS_LEVEL_MAX = 22;
    const ZSTD_COMPRESS_LEVEL_DEFAULT = 3;

    function zstd_compress_init(int $level = ZSTD_COMPRESS_LEVEL_DEFAULT, ?string $dict = null): \Zstd\Compress\Context|false {}
    function zstd_compress_add(\Zstd\Compress\Context $context, string $data, bool $end = false): string|false {}
    function zstd_uncompress_init(?string $dict = null): \Zstd\UnCompress\Context|false {}
    function zstd_uncompress_add(\Zstd\UnCompress\Context $context, string $data): string|false {}
}

namespace Zstd\Compress {
    class Context {}
}

namespace Zstd\UnCompress {
    class Context {}
}

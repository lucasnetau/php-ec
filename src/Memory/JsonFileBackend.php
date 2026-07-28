<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation\Memory;

use RuntimeException;

use function dirname;
use function file_exists;
use function file_put_contents;
use function file_get_contents;
use function gzdecode;
use function gzencode;
use function is_dir;
use function is_readable;
use function is_writable;
use function json_decode;
use function json_encode;
use function mkdir;
use function str_starts_with;
use function strlen;

use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;
use const LOCK_EX;

/**
 * JSON file backend for persisting collective memory entries.
 */
class JsonFileBackend implements MemoryBackendInterface
{
    public function __construct(
        private readonly string $filePath,
    ) {}

    public function load(): array
    {
        if (!file_exists($this->filePath)) {
            return [];
        }

        if (!is_readable($this->filePath)) {
            throw new RuntimeException("Memory backend file is not readable: {$this->filePath}");
        }

        $json = @file_get_contents($this->filePath);
        if ($json === false || $json === '') {
            return [];
        }

        if (str_starts_with($json, "\x1f\x8b")) {
            $json = gzdecode($json);
            if ($json === false) {
                return [];
            }
        }

        try {
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return [];
        }

        if (!is_array($data)) {
            return [];
        }

        $entries = [];
        foreach ($data as $item) {
            if (isset($item['namespace'], $item['key'], $item['value'])) {
                $entries[] = MemoryEntry::fromArray($item);
            }
        }

        return $entries;
    }

    public function save(array $entries): void
    {
        $dir = dirname($this->filePath);
        if (!is_dir($dir)) {
            if (!mkdir($dir, 0777, true)) {
                throw new RuntimeException("Unable to create memory backend directory: {$dir}");
            }
        }

        if (!is_writable($dir)) {
            throw new RuntimeException("Memory backend directory is not writable: {$dir}");
        }

        $data = [];
        foreach ($entries as $entry) {
            $data[] = $entry->jsonSerialize();
        }

        $json = json_encode($data, JSON_THROW_ON_ERROR);
        $compressed = gzencode($json, 2);
        file_put_contents($this->filePath, $compressed, LOCK_EX);
    }
}

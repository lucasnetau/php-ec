<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\JsonRpcLogger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\JsonRpcLogger
 */
class JsonRpcLoggerTest extends TestCase {

    public function testDestructOnUnopenedUrlStreamDoesNotError() : void {
        $file = tempnam(sys_get_temp_dir(), 'php-ec-logger-');
        @unlink($file);

        $logger = new JsonRpcLogger(LogLevel::DEBUG, $file);
        unset($logger);

        $this->assertFileDoesNotExist($file);
    }

    public function testDestructDoesNotCloseCallerSuppliedStream() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);
        unset($logger);

        $this->assertTrue(is_resource($stream), 'Logger must not close a caller-owned stream');
        fwrite($stream, 'still open');
        rewind($stream);
        $this->assertSame('still open', stream_get_contents($stream));
        fclose($stream);
    }

    public function testDestructClosesStreamItOpened() : void {
        $file = tempnam(sys_get_temp_dir(), 'php-ec-logger-');
        try {
            $logger = new JsonRpcLogger(LogLevel::DEBUG, $file);
            $logger->debug('hello');
            unset($logger);
            $this->assertStringContainsString('hello', file_get_contents($file));
        } finally {
            @unlink($file);
        }
    }
}

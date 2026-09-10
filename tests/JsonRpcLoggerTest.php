<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\JsonRpcLogger;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use RuntimeException;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\JsonRpcLogger
 */
class JsonRpcLoggerTest extends TestCase {

    /** @return array<int, array<string, mixed>> */
    private function readMessages($stream) : array {
        rewind($stream);
        $contents = trim((string)stream_get_contents($stream));
        if ($contents === '') {
            return [];
        }
        return array_map(static fn(string $line) => json_decode($line, true), explode("\n", $contents));
    }

    public function testRejectsNonResourceStream() : void {
        $this->expectException(InvalidArgumentException::class);
        new JsonRpcLogger(LogLevel::DEBUG, 12345);
    }

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

    public function testLevelFiltering() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::WARNING, $stream);

        $logger->debug('drop debug');
        $logger->notice('drop notice');
        $logger->warning('keep warning');
        $logger->error('keep error');

        $messages = $this->readMessages($stream);
        $this->assertCount(2, $messages);
        $this->assertSame('keep warning', $messages[0]['params']['message']);
        $this->assertSame('keep error', $messages[1]['params']['message']);
        fclose($stream);
    }

    public function testMessageInterpolation() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);

        $logger->debug('Sensor {sensor} reading {value}', ['sensor' => 'A1', 'value' => 42, 'unused' => 'kept']);

        $messages = $this->readMessages($stream);
        $this->assertCount(1, $messages);
        $this->assertSame('Sensor A1 reading 42', $messages[0]['params']['message']);
        $this->assertSame(['unused' => 'kept'], $messages[0]['params']['context']);
        fclose($stream);
    }

    public function testInterpolationStringifiesValues() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);
        $resource = fopen('php://memory', 'r');
        $withToString = new class() {
            public function __toString() : string {
                return 'stringable';
            }
        };

        $logger->debug(
            '{array}|{resource}|{date}|{stringable}|{plain}|{nothing}',
            [
                'array' => ['a' => 1],
                'resource' => $resource,
                'date' => new \DateTimeImmutable('2024-01-02T03:04:05+00:00'),
                'stringable' => $withToString,
                'plain' => new \stdClass(),
                'nothing' => null,
            ]
        );

        $messages = $this->readMessages($stream);
        $this->assertSame(
            '{"a":1}|{resource}|2024-01-02T03:04:05+00:00|stringable|{object stdClass}|',
            $messages[0]['params']['message']
        );
        $this->assertSame([], $messages[0]['params']['context']);
        fclose($resource);
        fclose($stream);
    }

    public function testExceptionContextIsLoggedAsExtraCriticalMessage() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);

        try {
            throw new RuntimeException('boom');
        } catch (RuntimeException $exception) {
            $logger->log(LogLevel::ERROR, 'failed', ['exception' => $exception, 'id' => 'job-1']);
        }

        $messages = $this->readMessages($stream);
        $this->assertCount(2, $messages);
        $this->assertSame('failed', $messages[0]['params']['message']);
        $this->assertSame(['id' => 'job-1'], $messages[0]['params']['context']);
        $this->assertSame(LogLevel::CRITICAL, $messages[1]['params']['logLevel']);
        $this->assertStringContainsString('RuntimeException: boom', $messages[1]['params']['message']);
        $this->assertStringContainsString('Stack trace:', $messages[1]['params']['message']);
        fclose($stream);
    }

    public function testNonThrowableInExceptionContextIsLeftAlone() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);

        $logger->error('failed', ['exception' => 'not a throwable']);

        $messages = $this->readMessages($stream);
        $this->assertCount(1, $messages);
        $this->assertSame(['exception' => 'not a throwable'], $messages[0]['params']['context']);
        fclose($stream);
    }

    public function testUnknownLevelDefaultsToError() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger(LogLevel::DEBUG, $stream);

        $logger->log('not-a-real-level', 'still logged');

        $messages = $this->readMessages($stream);
        $this->assertCount(1, $messages);
        $this->assertSame(LogLevel::ERROR, $messages[0]['params']['logLevel']);
        $this->assertSame('still logged', $messages[0]['params']['message']);
        fclose($stream);
    }

    public function testUnknownConfiguredLevelDefaultsToWarning() : void {
        $stream = fopen('php://memory', 'w+');
        $logger = new JsonRpcLogger('not-a-real-level', $stream);

        $logger->info('dropped');
        $logger->warning('kept');

        $messages = $this->readMessages($stream);
        $this->assertCount(1, $messages);
        $this->assertSame('kept', $messages[0]['params']['message']);
        fclose($stream);
    }
}

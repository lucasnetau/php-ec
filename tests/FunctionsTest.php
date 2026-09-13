<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\Rule\UndefinedRule;
use EdgeTelemetrics\EventCorrelation\Scheduler;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use RuntimeException;
use function EdgeTelemetrics\EventCorrelation\env;
use function EdgeTelemetrics\EventCorrelation\handleMissingClass;
use function EdgeTelemetrics\EventCorrelation\php_cmd;
use function EdgeTelemetrics\EventCorrelation\rpcCompressionAlgorithm;
use function EdgeTelemetrics\EventCorrelation\rpcCompressionAvailable;
use function EdgeTelemetrics\EventCorrelation\rpcLogMessage;
use function EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\env
 * @covers \EdgeTelemetrics\EventCorrelation\php_cmd
 * @covers \EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd
 * @covers \EdgeTelemetrics\EventCorrelation\handleMissingClass
 * @covers \EdgeTelemetrics\EventCorrelation\rpcLogMessage
 */
class FunctionsTest extends TestCase {

    public function testEnvReturnsValueAndDefault() : void {
        putenv('PHP_EC_FUNCTIONS_TEST=value');
        try {
            $this->assertSame('value', env('PHP_EC_FUNCTIONS_TEST'));
            $this->assertSame('fallback', env('PHP_EC_FUNCTIONS_TEST_MISSING', 'fallback'));
        } finally {
            putenv('PHP_EC_FUNCTIONS_TEST');
        }
    }

    public function testEnvThrowsWhenMissingWithoutDefault() : void {
        putenv('PHP_EC_FUNCTIONS_TEST_MISSING');
        $this->expectException(RuntimeException::class);
        env('PHP_EC_FUNCTIONS_TEST_MISSING');
    }

    public function testPhpCmdArrayForm() : void {
        $script = realpath(__DIR__ . '/../bin/save_state.php');
        $this->assertNotFalse($script);

        $cmd = php_cmd($script);
        $this->assertIsArray($cmd);
        $this->assertSame(PHP_BINARY, $cmd[0]);
        $this->assertContains($script, $cmd);
    }

    public function testPhpCmdStringFormEscapesArguments() : void {
        $script = realpath(__DIR__ . '/../bin/save_state.php');
        $cmd = php_cmd($script, false);
        $this->assertIsString($cmd);
        $this->assertStringContainsString(escapeshellarg($script), $cmd);
    }

    public function testPhpCmdFallsBackToScriptNotFound() : void {
        $cmd = php_cmd('/does/not/exist.php');
        $filename = $cmd[array_search('-f', $cmd, true) + 1];
        $this->assertStringEndsWith('script_not_found.php', $filename);
    }

    public function testWrapSourcePhpCmdIncludesPrepend() : void {
        $script = realpath(__DIR__ . '/../bin/save_state.php');
        $prepend = realpath(__DIR__ . '/../bin/source_prepend.php');
        $this->assertNotFalse($prepend);

        $cmd = wrap_source_php_cmd($script);
        $this->assertContains("auto_prepend_file=$prepend", $cmd);
        $this->assertContains($script, $cmd);

        $stringCmd = wrap_source_php_cmd($script, false);
        $this->assertStringContainsString('auto_prepend_file=' . escapeshellarg($prepend), $stringCmd);
    }

    public function testHandleMissingClassAliasesToUndefinedRule() : void {
        $className = 'PhpEcMissingRule_' . bin2hex(random_bytes(4));
        $log = tempnam(sys_get_temp_dir(), 'php-ec-errorlog-');
        $previous = ini_get('error_log');
        ini_set('error_log', $log);

        try {
            $this->assertFalse(class_exists($className, false));
            handleMissingClass($className);
            $this->assertTrue(class_exists($className, false), 'alias must be created');
            $this->assertTrue(is_a($className, UndefinedRule::class, true));
            $this->assertStringContainsString($className, (string)file_get_contents($log));
        } finally {
            ini_set('error_log', $previous === false ? '' : $previous);
            @unlink($log);
        }
    }

    public function testRpcLogMessageShape() : void {
        $notification = rpcLogMessage(LogLevel::INFO, 'hello {name}', ['name' => 'world']);

        $this->assertSame(Scheduler::RPC_PROCESS_LOG, $notification->getMethod());
        $this->assertSame(LogLevel::INFO, $notification->getParam('logLevel'));
        $this->assertSame('hello {name}', $notification->getParam('message'));
        $this->assertSame(['name' => 'world'], $notification->getParam('context'));
    }

    public function testRpcCompressionAlgorithmMapping() : void {
        $this->assertSame('deflate', rpcCompressionAlgorithm('1'));
        $this->assertSame('deflate', rpcCompressionAlgorithm('deflate'));
        $this->assertSame('zstd', rpcCompressionAlgorithm('zstd'));
        $this->assertSame('', rpcCompressionAlgorithm(''));
        $this->assertSame('', rpcCompressionAlgorithm('gzip'));
    }

    public function testRpcCompressionAvailableFallsBackOnlyForZstd() : void {
        $zstd = function_exists('zstd_compress_init');
        $this->assertSame($zstd ? 'zstd' : 'deflate', rpcCompressionAvailable('zstd'));
        $this->assertSame('deflate', rpcCompressionAvailable('1'));
        $this->assertSame('', rpcCompressionAvailable(''));
    }
}

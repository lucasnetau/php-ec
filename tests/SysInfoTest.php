<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Tests;

use EdgeTelemetrics\EventCorrelation\SysInfo;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * @covers \EdgeTelemetrics\EventCorrelation\SysInfo
 */
class SysInfoTest extends TestCase {

    private string|false $originalLimit;

    protected function setUp(): void {
        $this->originalLimit = ini_get('memory_limit');
    }

    protected function tearDown(): void {
        if ($this->originalLimit !== false) {
            @ini_set('memory_limit', $this->originalLimit);
        }
    }

    public function testUnlimitedPhpLimit() : void {
        ini_set('memory_limit', '-1');
        $this->assertSame(SysInfo::NO_LIMIT, (new SysInfo())->getPhpLimit());
    }

    public function testShorthandPhpLimit() : void {
        ini_set('memory_limit', '128M');
        $this->assertSame(128 * 1024 * 1024, (new SysInfo())->getPhpLimit());
    }

    public function testUnparseablePhpLimitIsTreatedAsUnlimited() : void {
        @ini_set('memory_limit', '1G ');
        if (ini_get('memory_limit') !== '1G ') {
            $this->markTestSkipped('This PHP build does not retain the unparseable memory_limit value');
        }

        $this->assertSame(SysInfo::NO_LIMIT, (new SysInfo())->getPhpLimit());
    }
}

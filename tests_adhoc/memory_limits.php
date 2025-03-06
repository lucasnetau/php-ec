<?php declare(strict_types=1);

include __DIR__ . '/../vendor/autoload.php';

$sysinfo = new \EdgeTelemetrics\EventCorrelation\SysInfo();

echo "Physical Memory: " . $sysinfo->getPhysicalMemoryTotal() . PHP_EOL;
echo "Configured PHP Limit: " . $sysinfo->getPhpLimit() . PHP_EOL;
echo "CGroup Limit: " . $sysinfo->getCgroupMemoryLimit() . PHP_EOL;
echo "Calculated Limit: " . $sysinfo->getMemoryLimit() . PHP_EOL;

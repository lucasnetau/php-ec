<?php declare(strict_types=1);

use function EdgeTelemetrics\EventCorrelation\initialiseSourceProcess;

require __DIR__ . '/../../../vendor/autoload.php';

initialiseSourceProcess(true);
<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\Library\Actions;

abstract class InvokableClassAction implements \Psr\Log\LoggerAwareInterface {
    use \Psr\Log\LoggerAwareTrait;
    abstract function __invoke();
}
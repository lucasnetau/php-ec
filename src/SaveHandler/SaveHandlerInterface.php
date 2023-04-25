<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\SaveHandler;
use Evenement\EventEmitterInterface;

interface SaveHandlerInterface extends EventEmitterInterface {

    public function saveStateAsync(array $state) : void;
    public function saveStateSync(array $state) : void;

    public function asyncSaveInProgress() : bool;

    public function lastSaveSizeBytes() : int;

    public function lastSaveWriteDuration() : int;

    public function loadState() : false|array;
}
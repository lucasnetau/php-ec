<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use Evenement\EventEmitterTrait;
use Evenement\EventEmitterInterface;

class JsonToEvent implements EventEmitterInterface
{
    use EventEmitterTrait;

    public function parse($data)
    {
        try {
            $event = new Event($data);
            $this->emit('data', [$event]);
        } catch (\Exception $ex) {
            echo "Error {$ex->getMessage()}\n";
            $this->emit('error', [$ex]);
        }
    }
}
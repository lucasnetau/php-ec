<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use DateTimeImmutable;
use JsonSerializable;
use Serializable;

interface IEvent extends JsonSerializable, Serializable {

    public function setReceivedTime(DateTimeImmutable $time);
    public function getDatetime() : ?DateTimeImmutable;

}
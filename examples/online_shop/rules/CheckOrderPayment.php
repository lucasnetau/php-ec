<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Event;
use EdgeTelemetrics\EventCorrelation\Rule;

class CheckOrderPayment extends Rule {
    const EVENTS = [['shop:order:placed'],['shop:order:paid']];

    const TIMEOUT = 'PT20S';

    const HISTORICAL_IGNORE_TIMEOUT = false;

    public function acceptEvent(Event $event) :bool
    {
        /** We will only handle matching for a single order so record this on the first entry and check it on the second event. */
        $this->context['orderid'] ??= $event->orderid;
        return ($this->context['orderid'] === $event->orderid);
    }

    public function onTimeout(): void
    {
        echo "Order {$this->context['orderid']} not paid on time, sending reminder to customer\n";
        $event = new Event(['event' => 'shop:order:payment:timeout']);
        $this->emit("data", [$event]);
        $this->actionFired = true; //Ensure we set this before sending off an event
    }
}
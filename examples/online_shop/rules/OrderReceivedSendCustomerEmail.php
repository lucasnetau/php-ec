<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Action;

class OrderReceivedSendCustomerEmail extends Rule {

    const EVENTS = [['shop:order:placed']];

    public function fire() : void
    {
        if ($this->complete() && !$this->actionFired)
        {
            $action = new Action("send-email", ['orderid' => $this->consumedEvents[0]->orderid, 'subject' => 'Order Confirmation']);
            $this->emit('data', [$action]);
            $this->actionFired = true;
        }
    }
}
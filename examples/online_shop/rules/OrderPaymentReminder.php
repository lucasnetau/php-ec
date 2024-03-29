<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Action;

class OrderPaymentReminder extends Rule {

    const EVENTS = [['Order:Payment:Timeout']];

    public function fire() : void
    {
        if ($this->complete() && !$this->actionFired)
        {
            $action = new Action("send-email", ['orderid' => $this->consumedEvents[0]->orderid, 'subject' => 'Order Payment Reminder']);
            $this->emit('data', [$action]);
            $this->actionFired = true;
        }
    }
}
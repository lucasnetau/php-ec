<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Action;

class OrderPaidSendCustomerReceipt extends Rule {

    const EVENTS = [['shop:order:paid']];

    public function fire()
    {
        if ($this->complete() && !$this->actionFired)
        {
            $action = new Action("send-email", ['orderid' => $this->consumedEvents[0]->orderid, 'subject' => 'Order Payment Receipt']);
            $this->emit('data', [$action]);
            $this->actionFired = true;
        }
    }
}
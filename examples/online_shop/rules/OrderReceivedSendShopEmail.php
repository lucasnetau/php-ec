<?php declare(strict_types=1);

use EdgeTelemetrics\EventCorrelation\Rule;
use EdgeTelemetrics\EventCorrelation\Action;

class OrderReceivedSendShopEmail extends Rule {

    const EVENTS = [['shop:order:placed']];

    public function onComplete() : void
    {
        $action = new Action("send-email", ['orderid' => $this->consumedEvents[0]->orderid, 'subject' => 'New Order Notification']);
        $this->emit('data', [$action]);
        $this->actionFired = true;
    }
}
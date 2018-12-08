<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

class Action implements \JsonSerializable {

    /**
     * @var string
     */
    protected $cmd;

    /**
     * @var array
     */
    protected $vars;

    public function __construct(string $cmd, array $vars)
    {
        $this->cmd = $cmd;
        $this->vars = $vars;
    }

    public function jsonSerialize()
    {
        return ['cmd' => $this->cmd,
                'vars' => $this->vars];
    }

    public function getCmd()
    {
        return $this->cmd;
    }

    public function getVars()
    {
        return $this->vars;
    }
}
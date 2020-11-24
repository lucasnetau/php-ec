<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use RuntimeException;
use JsonSerializable;

class Action implements JsonSerializable {

    /**
     * @var string
     */
    protected string $cmd;

    /**
     * @var array
     */
    protected array $vars;

    /**
     * Action constructor.
     * @param string $cmd
     * @param array|Event $vars
     */
    public function __construct(string $cmd, $vars)
    {
        $this->cmd = $cmd;
        if ($vars instanceof Event) {
            $this->vars = json_decode(json_encode($vars),true);
        } elseif (is_array($vars)) {
            $this->vars = $vars;
        } else {
            throw new RuntimeException("Invalid variables passed to Action constructor");
        }
    }

    /**
     * @return array
     */
    public function jsonSerialize() : array
    {
        return [
            'cmd' => $this->cmd,
            'vars' => $this->vars
        ];
    }

    /**
     * @return string
     */
    public function getCmd() : string
    {
        return $this->cmd;
    }

    /**
     * @return array
     */
    public function getVars() : array
    {
        return $this->vars;
    }
}
<?php declare(strict_types=1);

/*
 * This file is part of the PHP Event Correlation package.
 *
 * (c) James Lucas <james@lucas.net.au>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace EdgeTelemetrics\EventCorrelation;

use JsonSerializable;

/**
 * Class Action
 * @package EdgeTelemetrics\EventCorrelation
 */
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
    public function __construct(string $cmd, Event|array $vars)
    {
        $this->cmd = $cmd;
        if ($vars instanceof Event) {
            $this->vars = $vars->toArray();
        } else {
            $this->vars = $vars;
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
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

use Evenement\EventEmitterInterface;
use JsonSerializable;

/**
 * Class Action
 * @package EdgeTelemetrics\EventCorrelation
 */
class Action implements JsonSerializable, EventEmitterInterface {

    use \Evenement\EventEmitterTrait;

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
    public function jsonSerialize() : array {
        $vars = array_map(function($var) {
            if ($var instanceof \DateTimeInterface) {
                return $var->format(IEvent::DATETIME_SERIALISATION_FORMAT);
            }
            return $var;
        }, $this->vars);
        return [
            'cmd' => $this->cmd,
            'vars' => $vars
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
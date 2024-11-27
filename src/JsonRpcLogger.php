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

use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use React\Stream\WritableStreamInterface;
use function fopen;
use function fwrite;
use function get_class;
use function is_resource;
use function is_scalar;
use function is_string;
use function is_writable;
use function json_encode;
use function method_exists;
use function sprintf;
use function strpos;

/**
 * PSR-3 logger that logs via a JsonRpc call.
 * Based on MIT Licensed Bref\Logger (2019 Matthieu Napoli)
 */
class JsonRpcLogger extends AbstractLogger
{
    private const LOG_LEVEL_MAP = [
        LogLevel::EMERGENCY => 8,
        LogLevel::ALERT => 7,
        LogLevel::CRITICAL => 6,
        LogLevel::ERROR => 5,
        LogLevel::WARNING => 4,
        LogLevel::NOTICE => 3,
        LogLevel::INFO => 2,
        LogLevel::DEBUG => 1,
    ];

    /** @var string */
    private string $logLevel;

    /** @var string|null */
    private ?string $url;

    /** @var resource|WritableStreamInterface */
    private $stream;

    /**
     * @param string $logLevel The log level above which messages will be logged. Messages under this log level will be ignored.
     * @param resource|string|WritableStreamInterface $stream If unsure leave the default value.
     */
    public function __construct(string $logLevel = LogLevel::WARNING, $stream = STDOUT)
    {
        $this->logLevel = $logLevel;

        if (is_resource($stream) || $stream instanceof WritableStreamInterface) {
            $this->stream = $stream;
        } elseif (is_string($stream)) {
            $this->url = $stream;
        } else {
            throw new \InvalidArgumentException('A stream must either be a resource or a string.');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function log($level, $message, array $context = []): void
    {
        if (self::LOG_LEVEL_MAP[$level] < self::LOG_LEVEL_MAP[$this->logLevel]) {
            return;
        }

        $this->openStream();

        $message = $this->interpolate($message, $context);

        $message = rpcLogMessage($level, $message);

        $this->write($message);

        /**
         * If an Exception object is passed in the context data, it MUST be in the 'exception' key.
         * Logging exceptions is a common pattern and this allows implementors to extract a stack trace
         * from the exception when the log backend supports it. Implementors MUST still verify that
         * the 'exception' key is actually an Exception before using it as such, as it MAY contain anything.
         */
        if (isset($context['exception']) && $context['exception'] instanceof \Throwable) {
            $this->logException($context['exception']);
        }
    }

    private function openStream(): void
    {
        if ($this->stream !== null) {
            return;
        }
        $this->stream = fopen($this->url, 'a');
        if (! $this->stream) {
            throw new \RuntimeException('Unable to open stream ' . $this->url);
        }
    }

    private function write(JsonRpcNotification $message) {
        if ($this->stream instanceof \Clue\React\NDJson\Encoder) {
            $this->stream->write($message);
        } elseif ($this->stream instanceof WritableStreamInterface) {
            $this->stream->write(json_encode($message) . "\n");
        } elseif(is_resource($this->stream)) {
            $bytes = @fwrite($this->stream, json_encode($message) . "\n");
            if ($bytes === false) {
                fwrite(STDERR, json_encode($message) . "\n");
            }
        } else {
            fwrite(STDERR, json_encode($message) . "\n");
        }
    }

    /**
     * Interpolates context values into the message placeholders.
     */
    private function interpolate(string $message, array $context): string
    {
        if (strpos($message, '{') === false) {
            return $message;
        }

        $replacements = [];
        foreach ($context as $key => $val) {
            if ($val === null || is_scalar($val) || (\is_object($val) && method_exists($val, '__toString'))) {
                $replacements["{{$key}}"] = $val;
            } elseif ($val instanceof \DateTimeInterface) {
                $replacements["{{$key}}"] = $val->format(\DateTime::RFC3339);
            } elseif (\is_object($val)) {
                $replacements["{{$key}}"] = '{object ' . \get_class($val) . '}';
            } elseif (\is_resource($val)) {
                $replacements["{{$key}}"] = '{resource}';
            } else {
                $replacements["{{$key}}"] = json_encode($val);
            }
        }

        return strtr($message, $replacements);
    }

    private function logException(\Throwable $exception): void
    {
        $this->write(rpcLogMessage(LogLevel::CRITICAL, sprintf(
            "%s: %s in %s:%d, Stack trace: %s",
            get_class($exception),
            $exception->getMessage(),
            $exception->getFile(),
            $exception->getLine(),
            str_replace("\n", ", ", $exception->getTraceAsString())
        )));
    }
}

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

use EdgeTelemetrics\EventCorrelation\Rule\UndefinedRule;
use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use Nette\PhpGenerator\Parameter;
use Psr\Log\LogLevel;
use React\EventLoop\Loop;
use RuntimeException;

use Throwable;
use function error_get_last;
use function escapeshellarg;
use function file_exists;
use function function_exists;
use function fwrite;
use function is_resource;
use function json_encode;
use function posix_setpgid;
use function realpath;
use function register_shutdown_function;
use function set_exception_handler;
use const PHP_BINARY;

if (! function_exists('EdgeTelemetrics\EventCorrelation\disableOutputBuffering')) {

    function disableOutputBuffering() : void
    {
        /** Disable all output buffering */
        @ini_set('zlib.output_compression', '0');
        ini_set('output_buffering', '0');
        ini_set('implicit_flush', '1');
        if (PHP_VERSION_ID < 80000) {
            //PHP 7.4 and below used int as the flag
            /**
             * @noinspection PhpStrictTypeCheckingInspection
             * @phpstan-ignore-next-line
             * @psalm-suppress InvalidBooleanArgument
             */
            ob_implicit_flush(1);
        } else {
            //PHP8 and above now expect a boolean value
            /**
             * @noinspection PhpStrictTypeCheckingInspection
             * @phpstan-ignore-next-line
             * @psalm-suppress InvalidScalarArgument
             */
            ob_implicit_flush(true);
        }
        while (ob_get_level() > 0) {
            ob_end_flush();
        }
    }

}

if (! function_exists('EdgeTelemetrics\EventCorrelation\env')) {
    /**
     * Helper for referencing environment variables with defaults
     *
     * @param string $variableName The name of the environment variable.
     * @param ?string $defaultValue The default value to be used if the environment variable is not defined.
     * @return null|string
     * @throws RuntimeException If the variable was requested without a default value (non-optional) then this exception is thrown.
     */
    function env(string $variableName, ?string $defaultValue = null): ?string
    {
        // Only mark as optional if the default value was *explicitly* provided.
        $isOptional = (2 === func_num_args());

        $env = getenv($variableName);
        if ( false === $env )
        {
            if ( true === $isOptional ) {
                $env = $defaultValue;
            } else {
                throw new RuntimeException("Non-Optional ENV variable $variableName not set" );
            }
        }

        return $env;
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\checkpoint')) {
    function checkpoint($checkpoint) : void
    {
        $rpc = new JsonRpcNotification(Scheduler::INPUT_ACTION_CHECKPOINT, $checkpoint);
        fwrite(STDOUT, json_encode($rpc) . "\n");
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\php_cmd')) {
    function php_cmd(string $filename, bool $asArray = true): string|array
    {
        if (!file_exists($filename)) {
            $filename = realpath(__DIR__ . '/../bin/script_not_found.php');
        }
        if ($asArray) {
            return [
                PHP_BINARY,
                "-d",
                "display_errors=stderr",
                "-d",
                "log_errors=no",
                "-f",
                $filename,
                "--",
            ];
        } else {
            return escapeshellarg(PHP_BINARY) . " -d display_errors=stderr -d log_errors=no -f " . escapeshellarg($filename) . " --";
        }
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\wrap_source_php_cmd')) {
    function wrap_source_php_cmd(string $filename, bool $asArray = true): string|array
    {
        if (!file_exists($filename)) {
            $filename = realpath(__DIR__ . '/../bin/script_not_found.php');
        }
        $prepend_file = realpath(__DIR__ . '/../bin/source_prepend.php');
        if ($asArray) {
            return [
                PHP_BINARY,
                "-d",
                "auto_prepend_file=$prepend_file",
                "-d",
                "display_errors=stderr",
                "-d",
                "log_errors=no",
                "-f",
                $filename,
                "--",
            ];
        } else {
            return escapeshellarg(PHP_BINARY) . " -d auto_prepend_file=" . escapeshellarg($prepend_file) . " -d display_errors=stderr -d log_errors=no -f " . escapeshellarg($filename) . " --";
        }
    }
}

/**
 * @param string $unknownClassName
 */
if (! function_exists('EdgeTelemetrics\EventCorrelation\handleMissingClass')) {
    function handleMissingClass(string $unknownClassName) : void
    {
        error_log("Unable to autoload Rule $unknownClassName, generating an alias for cleaning up");
        /** Alias UndefinedRule to the unknown class */
        class_alias(UndefinedRule::class, $unknownClassName);
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\rpcLogMessage')) {
    /**
     * Helper function for an input or action process to send a log message to the parent scheduler
     * @param string $level LogLevel per \Psr\Log\LogLevel
     * @param string $message
     * @param array $context=[]
     * @return JsonRpcNotification
     */
    function rpcLogMessage(string $level, string $message, array $context = []): JsonRpcNotification {
        return new JsonRpcNotification(Scheduler::RPC_PROCESS_LOG, [
            'logLevel' => $level,
            'message' => $message,
            'context' => $context,
        ]);
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\setupErrorHandling')) {
    function setupErrorHandling(bool $usingEventLoop) : void
    {
        //Errors should be written to STDERR and not STDOUT. Disable log errors to prevent duplicate messages to STDERR
        ini_set('display_errors', 'stderr');
        ini_set('log_errors', 'no');
        if ($usingEventLoop) {
            Loop::get();
        }
        //Register these after the ReactPHP event loop is initialised via Loop::get() to ensure out shutdown function is always processed after the one registered there
        register_shutdown_function(function () {
            $last_error = error_get_last();
            if (($last_error['type'] ?? 0) & (E_ERROR | E_CORE_ERROR | E_COMPILE_ERROR | E_USER_ERROR | E_RECOVERABLE_ERROR)) {
                $error_message = "Fatal Error ({$last_error['file']}:{$last_error['line']}): {$last_error["message"]}";
                if (\STDOUT !== false && is_resource(\STDOUT)) {
                    fwrite(\STDOUT, json_encode(rpcLogMessage(LogLevel::EMERGENCY, $error_message)) . "\n");
                } else {
                    error_log($error_message);
                }
            }
        });
        //If any unhandled exception occur then log them to STDOUT (skip the and WritableStreamInterface $output) then terminate the Loop
        set_exception_handler(function (Throwable $exception) use ($usingEventLoop) {
            $error_message = "Process terminating on uncaught exception. " . $exception->getMessage() . "\n" . $exception->getTraceAsString();
            if (\STDOUT !== false && is_resource(\STDOUT)) {
                fwrite(\STDOUT, json_encode(rpcLogMessage(LogLevel::EMERGENCY, $error_message)) . "\n");
            } else {
                error_log($error_message);
            }
            if ($usingEventLoop) {
                Loop::stop();
            }
            exit(1);
        });
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\initialiseSourceProcess')) {
    function initialiseSourceProcess(bool $usingEventLoop) : void
    {
        disableOutputBuffering();
        setupErrorHandling($usingEventLoop);
        //Detach from the schedulers process group to ensure CTRL-C from shell isn't propagated
        if (function_exists('\posix_setpgid')) {
            posix_setpgid(0, 0);
        }
    }
}
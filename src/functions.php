<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation;

use EdgeTelemetrics\JSON_RPC\Notification as JsonRpcNotification;
use RuntimeException;

if (! function_exists('EdgeTelemetrics\EventCorrelation\disableOutputBuffering')) {

    function disableOutputBuffering()
    {
        /** Disable all output buffering */
        ini_set('zlib.output_compression', '0');
        ini_set('output_buffering', '0');
        ini_set('implicit_flush', '1');
        ob_implicit_flush(1);
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
     * @param mixed $defaultValue The default value to be used if the environment variable is not defined.
     * @return string
     * @throws RuntimeException If the variable was requested without a default value (non-optional) then this exception is thrown.
     */
    function env(string $variableName, $defaultValue = null)
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
    function checkpoint($checkpoint)
    {
        $rpc = new JsonRpcNotification(Scheduler::INPUT_ACTION_CHECKPOINT, $checkpoint);
        fwrite(STDOUT, json_encode($rpc) . "\n");
    }
}

if (! function_exists('EdgeTelemetrics\EventCorrelation\php_cmd')) {
    function php_cmd($filename)
    {
        if (!($php = getenv('PHP_BINARY')) || !is_executable($php)) {
            $php = "/usr/bin/php";
        }

        return escapeshellarg($php) . " -f " . escapeshellarg($filename) . " --";
    }
}
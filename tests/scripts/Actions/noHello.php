<?php declare(strict_types=1);

/**
 * Minimal JSON-RPC action that never sends rpc.hello, used to exercise the
 * scheduler's grace-period fallback for actions not using ActionHelper.
 */
while (($line = fgets(STDIN)) !== false) {
    $request = json_decode(trim($line), true);
    if (!is_array($request) || !isset($request['id'])) {
        continue;
    }
    fwrite(STDOUT, json_encode(['jsonrpc' => '2.0', 'id' => $request['id'], 'result' => true]) . "\n");
    fflush(STDOUT);
}

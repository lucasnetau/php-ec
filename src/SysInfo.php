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

use function array_filter;
use function count;
use function exec;
use function explode;
use function file_exists;
use function file_get_contents;
use function ini_get;
use function min;
use function preg_match;
use function strtoupper;
use function trim;

class SysInfo {

    const NO_LIMIT = -1;

    const CGROUP_FILE_PATHS = [
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroups v1 hard limit
        "/sys/fs/cgroup/memory/memory.soft_limit_in_bytes",  # cgroups v1 soft limit
        "/sys/fs/cgroup/memory.max",  # cgroups v2 hard limit
        "/sys/fs/cgroup/memory.high",  # cgroups v2 soft limit
    ];

    /**
     * @var int[] Memory values in Bytes (Good to 9223.37PB on 64-bit systems)
     */
    protected array $meminfo = [
        'physicalTotal' => self::NO_LIMIT,
    ];

    public function __construct() {
        $this->loadPhysicalMemInfo();
    }

    /**
     * Get the max allowable memory available to PHP based on the PHP memory limit, operating system, and physical limits
     * @return int
     */
    public function getMemoryLimit() : int {
        $limits = [
            $this->getPhpLimit(),
            $this->meminfo['physicalTotal'],
            $this->getCgroupMemoryLimit(),
        ];
        $limits = array_filter($limits, function($limit) { return $limit !== self::NO_LIMIT; });

        return (count($limits) === 0) ? self::NO_LIMIT : min($limits);
    }

    /**
     * Get the max allowable limit of memory based on physical memory and operating system limits (eg CGroups)
     * @return int
     */
    public function getAllowableMemoryLimit() : int
    {
        $limits = [
            $this->meminfo['physicalTotal'],
            $this->getCgroupMemoryLimit(),
        ];
        $limits = array_filter($limits, function($limit) { return $limit !== self::NO_LIMIT; });

        return (count($limits) === 0) ? self::NO_LIMIT : min($limits);
    }

    public function getPhpLimit() : int {
        $multiplierTable = ['K' => 1024, 'M' => 1024**2, 'G' => 1024**3];

        $memory_limit_setting = ini_get('memory_limit') ?: "-1";

        if ("-1" == $memory_limit_setting) {
            return self::NO_LIMIT;
        }

        preg_match("/^(-?[.0-9]+)([KMG])?$/i", $memory_limit_setting, $matches, PREG_UNMATCHED_AS_NULL);

        $bytes = (int)$matches[1];
        $multiplier = (null == $matches[2]) ? 1 : $multiplierTable[strtoupper($matches[2])];

        return $bytes * $multiplier;
    }

    public function getPhysicalMemoryTotal() : int {
        return $this->meminfo['physicalTotal'];
    }

    /**
     *
     */
    public function loadPhysicalMemInfo() : void {
        switch(PHP_OS_FAMILY) {
            case 'Linux':
                $multiplierTable = ['kB' => 1024,];

                $meminfo = @file_get_contents('/proc/meminfo');
                if ($meminfo === false) {
                    return;
                }
                foreach (explode("\n", trim($meminfo)) as $line) {
                    [$param, $value] = explode(":", $line);
                    $param = trim($param);
                    switch ($param) {
                        case 'MemTotal':
                            $value = explode(" ", trim($value));
                            $multiplier = (null == $value[1]) ? 1 : $multiplierTable[$value[1]];
                            $this->meminfo['physicalTotal'] = (int)$value[0] * $multiplier;
                            break 2;
                        default:
                            break;
                    }
                }
                break;
            case 'Darwin':
                exec('sysctl hw.memsize', $limit, $retval);
                if ($retval === 0) {
                    $limit = explode(" ", $limit[0]);
                    $this->meminfo['physicalTotal'] = (int)($limit[1] ?? self::NO_LIMIT);
                }
                break;
            case 'BSD':
                exec('sysctl hw.physmem', $limit, $retval);
                if ($retval === 0) {
                    $limit = explode(" ", $limit[0]);
                    $this->meminfo['physicalTotal'] = (int)($limit[1] ?? self::NO_LIMIT);
                }
                break;

            default:
                error_log('Unsupported Operating System Family ' . PHP_OS_FAMILY . ' detected, cannot detected physical memory total');
                return;
        }
    }

    /**
     * Load CGroup memory limits if set, honouring Soft limit
     * https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
     * @return int
     */
    public function getCgroupMemoryLimit() : int {
        if (PHP_OS_FAMILY === 'Darwin') {
            return self::NO_LIMIT;
        }
        $limit = PHP_INT_MAX;
        foreach(self::CGROUP_FILE_PATHS as $path) {
            if (file_exists($path)) {
                $cgroup_limit = @file_get_contents($path);
                if ($cgroup_limit === false || $cgroup_limit === "max\n") {
                    continue;
                }
                $limit = min($limit, (int)$cgroup_limit);
            }
        }

        return ($limit === PHP_INT_MAX) ? self::NO_LIMIT : $limit;
    }
}
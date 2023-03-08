<?php declare(strict_types=1);

namespace EdgeTelemetrics\EventCorrelation\SaveHandler;

use Evenement\EventEmitterTrait;
use Exception;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Filesystem\Filesystem;
use React\Filesystem\FilesystemInterface;
use RuntimeException;
use function error_get_last;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function json_decode;
use function json_encode;
use function json_last_error_msg;
use function rename;
use function round;
use function strlen;
use function tempnam;
use function unlink;

class FileAdapter implements SaveHandlerInterface {
    use EventEmitterTrait;

    protected FilesystemInterface $filesystem;

    /**
     * @var bool Flag to ensure we only have one save going on at a time
     */
    protected bool $asyncSaveInProgress = false;

    /**
     * @var int Size of the save state file
     */
    protected int $saveStateSizeBytes = 0;

    /** @var int Time in millisecond for last save handler to complete */
    protected int $saveStateLastDuration = 0;


    public function __construct( protected string $savePath, protected string $saveFileName, protected LoggerInterface $logger, protected ?LoopInterface $loop ) {
        $this->loop ??= Loop::get();
        $this->filesystem = Filesystem::create($this->loop);

    }

    public function saveStateAsync(array $state)
    {
        $this->asyncSaveInProgress = true;
        $filename = tempnam($this->savePath, ".php-ce.state.tmp");
        if (false === $filename) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Error creating temporary save state file, check filesystem")] );
            return;
        }
        $file = $this->filesystem->file($filename);
        $state = json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
        if ($state === false) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Encoding application state failed. " . json_last_error_msg())] );
            return;
        }
        $saveStateSize = strlen($state);
        $saveStateBegin = hrtime(true);
        $file->putContents($state)->then(function () use ($file, $saveStateSize, $saveStateBegin) {
            $file->rename($this->saveFileName)->then(function (\React\Filesystem\Node\FileInterface $newfile) use ($saveStateSize, $saveStateBegin) {
                //Everything Good
                $this->asyncSaveInProgress = false;
                $this->saveStateSizeBytes = $saveStateSize;
                $this->saveStateLastDuration = (int)round((hrtime(true) - $saveStateBegin)/1e+6); //Milliseconds
                if ($this->saveStateLastDuration > 5000) {
                    $this->logger->warning('It took ' . $this->saveStateLastDuration . ' milliseconds to save state to disk');
                }
            });
        }, function (Exception $ex) use($filename) {
            $this->emit('save:failed', ['exception' => $ex] ); /** We didn't save state correctly, so we mark the scheduler as dirty to ensure it is attempted again */
            $this->asyncSaveInProgress = false;
            if (file_exists($filename) && !unlink($filename)) {
                $this->logger->warning('Unable to delete temporary save file');
            }
        });
    }

    public function saveStateSync(array $state)
    {
        $filename = tempnam("/tmp", ".php-ce.state.tmp");
        if (false === $filename) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Error creating temporary save state file, check filesystem")] );
            return;
        }
        $state = json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
        if ($state === false) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Encoding application state failed. " . json_last_error_msg())] );
            return;
        }
        $saveStateSize = strlen($state);
        $saveStateBegin = hrtime(true);
        if (!(@file_put_contents($filename, $state) === $saveStateSize && rename($filename, $this->saveFileName))) {
            $this->emit('save:failed', ['exception' => new RuntimeException("Save state sync failed." . json_encode(error_get_last()))] );
            if (file_exists($filename) && !unlink($filename)) {
                $this->logger->warning('Unable to delete temporary save file');
            }
            return;
        }
        $this->saveStateLastDuration = (int)round((hrtime(true) - $saveStateBegin)/1e+6); //Milliseconds
        $this->saveStateSizeBytes = $saveStateSize;

        if ($this->saveStateLastDuration > 5000) {
            $this->logger->warning('It took ' . $this->saveStateLastDuration . ' milliseconds to save state to disk');
        }
        $this->logger->debug('State saved to filesystem');
    }

    public function asyncSaveInProgress(): bool
    {
        return $this->asyncSaveInProgress;
    }

    public function lastSaveSizeBytes() : int {
        return $this->saveStateSizeBytes;
    }

    public function lastSaveWriteDuration() : int {
        return $this->saveStateLastDuration;
    }

    public function loadState() : false|array {
        if (file_exists($this->saveFileName))
        {
            $contents = file_get_contents($this->saveFileName);
            if ($contents === false || $contents === '') {
                throw new RuntimeException('State file exists but contents could not be read or were empty. Exiting');
            }
            $state = json_decode($contents, true);
            if ($state === null) {
                throw new RuntimeException('Save state file was corrupted, Exiting. JSON Error: ' . json_last_error_msg() );
            }
            return $state;
        }
        return false;

    }
}
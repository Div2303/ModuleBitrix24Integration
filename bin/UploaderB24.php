<?php

namespace Modules\ModuleBitrix24Integration\bin;
require_once 'Globals.php';

use MikoPBX\Core\System\BeanstalkClient;
use Exception;
use MikoPBX\Core\Workers\WorkerBase;
use Modules\ModuleBitrix24Integration\Lib\Bitrix24Integration;
use Modules\ModuleBitrix24Integration\Lib\Logger;

class UploaderB24 extends WorkerBase
{
    public const B24_UPLOADER_CHANNEL = 'b24-uploader';
    private Bitrix24Integration $b24;
    private BeanstalkClient $queueAgent;
    private Logger $logger;
    private const FILE_POLL_INTERVAL = 2;      // секунд между проверками
    private const MAX_ATTEMPTS = 10;            // максимум попыток
    private const BASE_DELAY = 5;               // начальная задержка (сек)
    private const AUTH_RETRY_DELAY = 30;        // задержка при ошибке авторизации

    public function start($argv): void
    {
        $this->logger = new Logger('UploaderB24', 'ModuleBitrix24Integration');
        $this->logger->writeInfo('Start daemon...');

        $this->b24 = new Bitrix24Integration('_uploader');
        $this->initBeanstalk();

        $this->logger->writeInfo('Start waiting...');
        while (!$this->needRestart) {
            try {
                $this->queueAgent->wait(self::FILE_POLL_INTERVAL);
            } catch (Exception $e) {
                $this->logger->writeError($e->getLine() . ';' . $e->getCode() . ';' . $e->getMessage());
                sleep(1);
                $this->initBeanstalk();
            }
            $this->logger->rotate();
        }
    }

    private function initBeanstalk(): void
    {
        $this->logger->writeInfo('Init Beanstalk...');
        $this->queueAgent = new BeanstalkClient(self::B24_UPLOADER_CHANNEL);
        $this->queueAgent->subscribe($this->makePingTubeName(self::class), [$this, 'pingCallBack']);
        $this->queueAgent->subscribe(self::B24_UPLOADER_CHANNEL, [$this, 'processTask']);
        $this->queueAgent->setTimeoutHandler([$this, 'idleHandler']);
    }

    /**
     * Обработка задачи из очереди
     */
    public function processTask(BeanstalkClient $client): void
{
    $raw = $client->getBody();
    $this->logger->writeInfo("Received task: $raw");

    $task = json_decode($raw, true);
    if (!is_array($task) || empty($task['uploadUrl']) || empty($task['FILENAME'])) {
        $this->logger->writeError('Invalid task data');
        return; // задача будет удалена автоматически
    }

    $filename   = $task['FILENAME'];
    $uploadUrl  = $task['uploadUrl'];
    $attempts   = (int)($task['attempts'] ?? 0);
    $callId     = $task['CALL_ID'] ?? '';

    // Проверка существования файла
    if (!file_exists($filename)) {
        $this->logger->writeError("File not found: $filename");
        $this->scheduleRetry($client, $task, $attempts + 1, self::BASE_DELAY);
        return;
    }

    try {
        $result = $this->b24->uploadRecord($uploadUrl, $filename);
        $errorName = $result['error'] ?? '';

        // Ошибки авторизации – обновляем токен и откладываем без увеличения попыток
        if (in_array($errorName, ['expired_token', 'wrong_client', 'NO_AUTH_FOUND', 'invalid_token'], true)) {
            $this->logger->writeError("Auth error '$errorName', refreshing token and will retry");
            $this->b24->updateToken();
            $this->scheduleRetry($client, $task, $attempts, self::AUTH_RETRY_DELAY);
            return;
        }

        // Успех
        if (isset($result['result']['FILE_ID'])) {
            $this->logger->writeInfo("Upload successful for $filename");
            // задача будет удалена автоматически
            return;
        }

        // Прочие ошибки API
        $errorMsg = json_encode($result);
        $this->logger->writeError("Upload API error: $errorMsg");
        $this->scheduleRetry($client, $task, $attempts + 1, $this->getDelay($attempts));

    } catch (Exception $e) {
        $this->logger->writeError("Exception: " . $e->getMessage());
        $this->scheduleRetry($client, $task, $attempts + 1, $this->getDelay($attempts));
    }
}

private function scheduleRetry(BeanstalkClient $client, array $task, int $newAttempts, int $delay): void
{
    if ($newAttempts >= self::MAX_ATTEMPTS) {
        $this->logger->writeError("Task failed after {$newAttempts} attempts: " . json_encode($task));
        // задача будет удалена автоматически
        return;
    }

    $task['attempts'] = $newAttempts;
    $newBody = json_encode($task);
    // release задачу с задержкой (в секундах)
    $client->release($newBody, $delay);
    $this->logger->writeInfo("Task rescheduled with delay {$delay}s, attempts: {$newAttempts}");
}

    /**
     * Экспоненциальная задержка: 2^attempts * BASE_DELAY, но не более 1 часа
     */
    private function getDelay(int $attempts): int
    {
        $delay = self::BASE_DELAY * pow(2, $attempts);
        return (int)min($delay, 3600);
    }

    public function idleHandler(): void
    {
        // Ничего не делаем, просто чтобы не было ошибок
    }

    public function pingCallBack(BeanstalkClient $client): void
    {
        if ($this->needRestart) {
            return;
        }
        parent::pingCallBack($client);
    }

    public function signalHandler(int $signal): void
    {
        parent::signalHandler($signal);
        cli_set_process_title('SHUTDOWN_' . cli_get_process_title());
        if ($this->logger) {
            $this->logger->writeInfo("NEED SHUTDOWN ($signal)...");
        }
    }
}

// Start worker process
if (isset($argv) && count($argv) !== 1) {
    UploaderB24::startWorker($argv ?? []);
}
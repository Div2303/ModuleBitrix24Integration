<?php
/*
 * MikoPBX - free phone system for small business
 * Copyright © 2017-2022 Alexey Portnov and Nikolay Beketov
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

namespace Modules\ModuleBitrix24Integration\bin;
require_once 'Globals.php';

use MikoPBX\Core\System\BeanstalkClient;
use Exception;
use MikoPBX\Core\System\Processes;
use MikoPBX\Core\System\Util;
use MikoPBX\Core\Workers\WorkerBase;
use Modules\ModuleBitrix24Integration\Lib\Bitrix24Integration;
use Modules\ModuleBitrix24Integration\Lib\CacheManager;

class WorkerBitrix24IntegrationHTTP extends WorkerBase
{
    private Bitrix24Integration $b24;
    private $pidSyncProcContacts;
    private $timeSyncProcContacts;
    private array $q_req = [];
    private bool $need_get_events = false;
    private int $last_update_inner_num = 0;
    private BeanstalkClient $queueAgent;

    private bool  $searchEntities = false;
    private array $tmpCallsData = [];
    private array $didUsers = [];
    private array $perCallQueues = [];
    private bool  $hasPendingEvents = false;
    private bool  $insideExecuteTasks = false;
    private string $processState = 'init';

    public function signalHandler(int $signal): void
    {
        parent::signalHandler($signal);
        cli_set_process_title("SHUTDOWN[{$this->processState}]_" . self::class);
    }

    public function start($argv): void
    {
        $this->b24 = new Bitrix24Integration();
        if (!$this->b24->initialized) {
            die('Settings not set...');
        }
        $this->b24->mainLogger->writeInfo('Starting...');
        $this->b24->checkNeedUpdateToken();

        // ========== ОТКЛЮЧЕНА СИНХРОНИЗАЦИЯ ВНЕШНИХ ЛИНИЙ ==========
        // $externalLines = $this->b24->syncExternalLines();
        // foreach ($externalLines as $line){ ... }
        // $this->searchEntities = !empty($this->didUsers);
        // ==========================================================

        // Вместо этого просто отключаем поиск сущностей (он будет через REST)
        $this->searchEntities = false; // поиск сущностей будем делать напрямую через API

        pcntl_signal(SIGCHLD, SIG_IGN);
        pcntl_signal(SIGALRM, function () {}, false);

        $this->initBeanstalk();
        $this->processState = 'idle';
        while ($this->needRestart === false) {
            try {
                $this->processState = 'beanstalk_wait';
                pcntl_alarm(5);
                $timeout = $this->hasPendingEvents ? 0 : 1;
                $this->queueAgent->wait($timeout);
                pcntl_alarm(0);
                $this->processState = 'idle';
            } catch (Exception $e) {
                pcntl_alarm(0);
                $this->processState = 'reconnect';
                sleep(1);
                $this->initBeanstalk();
                $this->processState = 'idle';
            }
        }
    }

    private function initBeanstalk(): void
    {
        $this->queueAgent = new BeanstalkClient(Bitrix24Integration::B24_INTEGRATION_CHANNEL);
        $this->queueAgent->subscribe($this->makePingTubeName(self::class), [$this, 'pingCallBack']);
        $this->queueAgent->subscribe(Bitrix24Integration::B24_INTEGRATION_CHANNEL, [$this, 'b24ChannelCallBack']);
        $this->queueAgent->subscribe(Bitrix24Integration::B24_SEARCH_CHANNEL, [$this, 'b24ChannelSearch']);
        $this->queueAgent->subscribe(Bitrix24Integration::B24_INVOKE_REST_CHANNEL, [$this, 'invokeRest']);
        $this->queueAgent->setTimeoutHandler([$this, 'executeTasks']);
    }

    public function pingCallBack(BeanstalkClient $message): void
    {
        if($this->needRestart) return;
        $this->b24->mainLogger->writeInfo('Get ping event...');
        parent::pingCallBack($message);
    }

    public function invokeRest($client): void
    {
        $data = json_decode($client->getBody(), true);
        $action = $data['action']??'';
        $arg = [];
        if($action === 'scope'){
            $arg = $this->b24->getScopeAsync($data['inbox_tube']??'');
        }elseif($action === 'needRestart'){
            $this->needRestart = true;
        }
        if(!empty($arg)){
            $this->b24->mainLogger->writeInfo($data, "Add action $action in queue...");
            $this->q_req = array_merge($this->q_req, $arg);
        }
    }

    public function invokeRestCheckResponse($response,$tube, $partResponse): void
    {
        $this->b24->mainLogger->writeInfo([$response, $partResponse],"Response to tube $tube");
        $resFile = ConnectorDb::saveResultInTmpFile($partResponse);
        $this->queueAgent->publish($resFile, $tube);
    }

    public function b24ChannelSearch($client): void
    {
        $data = json_decode($client->getBody(), true);
        $this->createTmpCallData($data);
    }

    public function b24ChannelCallBack($client): void
    {
        $srcData = $client->getBody();
        try {
            $data = json_decode($srcData, true, 512, JSON_THROW_ON_ERROR);
        }catch (Exception $e){
            $this->b24->mainLogger->writeInfo('AMI Event'. $e->getMessage());
            return;
        }
        $linkedId = $data['linkedid']??'';
        if(empty($linkedId)){
            $this->b24->mainLogger->writeError($data, 'Get AMI, EMPTY linkedid');
            return;
        }
        $this->b24->mainLogger->writeInfo($data, 'Get AMI Event');
        if ($this->searchEntities
            && !isset($this->tmpCallsData[$linkedId])
            && $data['action'] === 'telephonyExternalCallRegister') {
            $this->createTmpCallData($data);
        }
        if(!isset($this->perCallQueues[$linkedId])){
            $this->perCallQueues[$linkedId] = new \SplQueue();
        }
        $this->perCallQueues[$linkedId]->enqueue($data);
    }

    private function addDataToQueue(array $data): void
    {
        if ('telephonyExternalCallRegister' === $data['action']) {
            $cache_key = 'tmp10' . __FUNCTION__ . $data['UNIQUEID'] . '_' . $data['USER_PHONE_INNER'];
            $res_data = $this->b24->getCache($cache_key);
            if ($res_data === null) {
                $this->b24->saveCache($cache_key, $data);

                $pre_call_key = "tmp5_{$data['USER_PHONE_INNER']}_" . Bitrix24Integration::getPhoneIndex($data['PHONE_NUMBER']);
                $cache_data = $this->b24->getCache($pre_call_key);
                if ($cache_data !== null) {
                    $data['PHONE_NUMBER'] = $cache_data['PHONE_NUMBER'] ?? $data['PHONE_NUMBER'];
                }
                $pre_call_key = "tmp5_ONEXTERNALCALLBACKSTART_" . Bitrix24Integration::getPhoneIndex($data['PHONE_NUMBER']);
                $cache_data = $this->b24->getCache($pre_call_key);
                if ($cache_data !== null) {
                    $data['PHONE_NUMBER']    = $cache_data['PHONE_NUMBER'] ?? $data['PHONE_NUMBER'];
                    $data['CRM_ENTITY_ID']   = $cache_data['CRM_ENTITY_ID'] ?? '';
                    $data['CRM_ENTITY_TYPE'] = $cache_data['CRM_ENTITY_TYPE'] ?? '';
                } else {
                    // ========== ИСПОЛЬЗУЕМ ПОИСК СУЩНОСТИ ЧЕРЕЗ REST ==========
                    $entity = $this->b24->searchEntitiesByPhone($data['PHONE_NUMBER']);
                    if (!empty($entity['id'])) {
                        $data['CRM_ENTITY_TYPE'] = $entity['type'];
                        $data['CRM_ENTITY_ID']   = $entity['id'];
                    }
                }

                $arg = [];
                $callId = &$this->tmpCallsData[$data['linkedid']]['CALL_ID'];
                if(empty($callId)){
                    $this->tmpCallsData[$data['linkedid']]['ARG_REGISTER_USER_'.$data['UNIQUEID']] = $data['USER_ID']??'';
                    [$arg, $key] = $this->b24->telephonyExternalCallRegister($data);
                    if(!empty($key)){
                        $callId = $key;
                    }
                }elseif(stripos($callId,Bitrix24Integration::API_CALL_REGISTER) === false){
                    $this->tmpCallsData[$data['linkedid']]['ARG_REGISTER_USER_'.$data['UNIQUEID']] = $data['USER_ID']??'';
                    $this->tmpCallsData[$data['linkedid']]['ARGS_REGISTER_'.$data['UNIQUEID']] = $this->b24->telephonyExternalCallRegister($data);
                    $data['CALL_ID'] = $callId;
                    if($this->needShowCardDirectly($data['USER_ID'])){
                        $arg = $this->b24->telephonyExternalCallShow($data);
                    }
                }else{
                    $this->tmpCallsData[$data['linkedid']]['ARG_REGISTER_USER_'.$data['UNIQUEID']] = $data['USER_ID']??'';
                    $this->tmpCallsData[$data['linkedid']]['ARGS_REGISTER_'.$data['UNIQUEID']] = $this->b24->telephonyExternalCallRegister($data);
                    $data['CALL_ID'] = '$result['.$callId.'][CALL_ID]';
                    if($this->needShowCardDirectly($data['USER_ID'])){
                        $arg = $this->b24->telephonyExternalCallShow($data);
                    }
                }
                if (count($arg) > 0) {
                    $this->q_req = array_merge($this->q_req, $arg);
                }
                unset($callId);
            }
        } elseif ('action_hangup_chan' === $data['action']) {
            $number = $this->parsePJSIP($data['channel']);
            $callData = $this->tmpCallsData[$data['linkedid']] ?? [];
            $data['CALL_ID'] = $callData['CALL_ID']??'';
            $data['USER_ID'] = $this->b24->inner_numbers[$number]['ID']??'';
            if (!empty($data['CALL_ID']) && !empty($data['USER_ID'])) {
                $arg = $this->b24->telephonyExternalCallHide($data);
                $this->q_req = array_merge($this->q_req, $arg);
            }
        } elseif ('action_dial_answer' === $data['action']) {
            $tmpArr = [];
            $userId = $this->tmpCallsData[$data['linkedid']]['ARG_REGISTER_USER_'.$data['UNIQUEID']]??'';
            $dealId = '';
            $leadId = '';
            $filter = [
                "linkedid='{$data['linkedid']}'",
                'order' => 'uniq_id'
            ];
            $b24CdrRows = ConnectorDb::invoke(ConnectorDb::FUNC_GET_CDR_BY_FILTER, [$filter]);
            foreach ($b24CdrRows as $cdrData) {
                $row = (object)$cdrData;
                $cdr = $row;
                if (!empty($cdr->dealId)) {
                    $dealId = max($dealId, $cdr->dealId);
                }
                if (!empty($cdr->lead_id)) {
                    $leadId = max($leadId, $cdr->lead_id);
                }
                $cdr->answer = 1;
                ConnectorDb::invoke(ConnectorDb::FUNC_UPDATE_FROM_ARRAY_CDR_BY_UID, [$row->uniq_id, (array)$cdr]);
                if (intval($userId) !== intval($row->user_id)) {
                    $data['CALL_ID'] = $row->call_id;
                    $data['USER_ID'] = (int)$userId;
                    if($this->needShowCardOnAnswer($userId)){
                        $tmpArr[] = $this->b24->telephonyExternalCallShow($data);
                    }
                }
            }
            if (!empty($leadId) && !empty($userId)) {
                $tmpArr[] = $this->b24->crmLeadUpdate($leadId, $userId, $data['linkedid']);
            }
            if(($this->tmpCallsData[$data['linkedid']]['crm-data']['CRM_ENTITY_TYPE']??'') === 'LEAD'
               && !isset($this->tmpCallsData[$data['linkedid']]['crm-data']['ID'])){
                if(!empty($userId)){
                    $tmpArr[] = $this->b24->crmLeadUpdate($this->tmpCallsData[$data['linkedid']]['crm-data']['CRM_ENTITY_ID'], $userId, $data['linkedid']);
                }else{
                    $this->b24->mainLogger->writeInfo($data, "Error empty userId, can not update lead ($data[linkedid])");
                }
            }
            if(!empty($tmpArr)){
                $this->q_req = array_merge($this->q_req, ...$tmpArr);
            }
        } elseif ('telephonyExternalCallFinish' === $data['action']) {
            [$arg,$finishKey] = $this->b24->telephonyExternalCallFinish($data, $this->tmpCallsData);
            $this->q_req = array_merge($this->q_req, $arg);

            if(!empty($finishKey)){
                $arg = $this->b24->crmActivityUpdate('$result['.$finishKey.'][CRM_ACTIVITY_ID]', $data['linkedid'], $data['linkedid']);
                $this->q_req = array_merge($this->q_req, $arg);
            }
        }else{
            $this->b24->mainLogger->writeInfo($data, "The event handler was not found ($data[linkedid])");
        }

        if (!$this->insideExecuteTasks && count($this->q_req) >= 49) {
            $this->executeTasks();
        }
    }

    private function parsePJSIP($s):?string
    {
        if (strpos($s, 'PJSIP/') === 0) {
            $s = substr($s, 6);
        }else{
            return null;
        }
        $parts = explode('-', $s);
        if (count($parts) < 2) {
            return null;
        }
        array_pop($parts);
        return implode('-', $parts);
    }

    private function needShowCardDirectly($userId): bool
    {
        $mode = $this->getUserOpenCardMode($userId);
        return $mode === Bitrix24Integration::OPEN_CARD_DIRECTLY || $mode === '';
    }

    private function needShowCardOnAnswer($userId): bool
    {
        return $this->getUserOpenCardMode($userId) === Bitrix24Integration::OPEN_CARD_ANSWERED;
    }

    private function getUserOpenCardMode($userId): string
    {
        $tmpInnerNumArray = array_values($this->b24->inner_numbers);
        $index = array_search($userId, array_column($tmpInnerNumArray, 'ID'), true);
        if ($index === false) {
            return '';
        }
        $innerNumber = $tmpInnerNumArray[$index]['UF_PHONE_INNER'] ?? '';
        return $this->b24->usersSettingsB24[$innerNumber]['open_card_mode'] ?? '';
    }

    public function shouldDeferForPreAction(&$data): bool
    {
        $action = $data['action']??'';
        $id     = $data['linkedid']??'';

        if($data['UNIQUEID'] === ''){
            $this->b24->mainLogger->writeError($data, "Empty UID $id...");
            return false;
        }
        $needActions = true;
        if ($this->searchEntities) {
            if (!isset($this->tmpCallsData[$id]) && $action === 'telephonyExternalCallRegister') {
                $this->createTmpCallData($data);
            }
            $callData = &$this->tmpCallsData[$id];
            if ($action === 'telephonyExternalCallRegister'
                && ($callData['data']['action']??'') !== 'telephonyExternalCallRegister'){
                $callData['data'] = $data;
                $data['CRM_ENTITY_TYPE'] = $callData['crm-data']['CRM_ENTITY_TYPE'];
                $data['CRM_ENTITY_ID']   = $callData['crm-data']['CRM_ENTITY_ID'];
            }
            $wait = $callData['wait']?? false;
            if ($wait === false) {
                $this->b24->mainLogger->writeInfo($data, "Process (1) $id...");
                $needActions = false;
            }else{
                $this->b24->mainLogger->writeInfo($data, "Event wait call register(2)... $id: ");
            }
        } else {
            $this->b24->mainLogger->writeInfo($data, "Process (2) $id...");
            $needActions = false;
        }
        return $needActions;
    }

    /**
     * Создаёт временные данные для вызова, но без синхронизации справочников.
     */
    private function createTmpCallData($data):void
    {
        if(isset($this->tmpCallsData[$data['linkedid']])){
            return;
        }
        $this->tmpCallsData[$data['linkedid']] = [
            'wait'       => true,
            'events'     => [],
            'search'     => -1,
            'lead'       => -1,
            'list-lead'  => -1,
            'company'    => -1,
            'data'       => $data,
            'crm-data'   => [],
            'inbox_tube' => $data['inbox_tube']??'',
            'responsible'=> '',
            'CALL_ID'    => '',
        ];
        $phone = $data['PHONE_NUMBER'] ?? '';
        if(empty($phone)){
            $this->b24->mainLogger->writeError($data, 'Empty phone number... ');
        } else {
            $this->findEntitiesByPhone($phone, $data['linkedid']);
        }
    }

    /**
     * Поиск сущности по номеру телефона через REST (без локальной БД).
     */
    public function findEntitiesByPhone(string $phone, string $linkedId = ''):void
    {
        $callData = &$this->tmpCallsData[$linkedId];

        // Выполняем поиск через REST
        $entity = $this->b24->searchEntitiesByPhone($phone);
        if (!empty($entity['id'])) {
            $callData['crm-data'] = [
                'CRM_ENTITY_TYPE' => $entity['type'],
                'CRM_ENTITY_ID'   => $entity['id'],
            ];
            $callData['responsible'] = $entity['assigned_by_id'];
            $callData['wait'] = false;
        } else {
            // Сущность не найдена – создаём лид, если включено
            $callData['wait'] = false;
            if ($this->crmCreateLead) {
                $did = $callData['data']['did'] ?? '';
                $userNum = $this->didUsers[$did][0] ?? '';
                $l_phone = $callData['data']['PHONE_NUMBER'] ?? '';
                $l_id    = $callData['data']['linkedid'] ?? '';
                $l_user  = $this->b24->inner_numbers[$userNum]['ID'] ?? '';
                $l_did   = $callData['data']['did'] ?? '';
                $arg = $this->b24->crmAddLead($l_phone, $l_id, $l_user, $l_did);
                $this->q_req = array_merge($arg, $this->q_req);
            }
        }

        if (!empty($callData['inbox_tube'])) {
            $this->queueAgent->publish(json_encode($callData), $callData['inbox_tube']);
            $callData['inbox_tube'] = '';
        }
    }

    public function handleEvent($result):void
    {
        // Обработка событий (оставляем, но без синхронизации справочников)
        $eventActionsDelete = [
            'ONCRMLEADDELETE'    => 'LEAD',
            'ONCRMCONTACTDELETE' => 'CONTACT',
            'ONCRMCOMPANYDELETE' => 'COMPANY',
        ];
        $eventActionsUpdate = [
            'ONCRMLEADUPDATE'    => Bitrix24Integration::API_CRM_LIST_LEAD,
            'ONCRMCONTACTUPDATE' => Bitrix24Integration::API_CRM_LIST_CONTACT,
            'ONCRMCOMPANYUPDATE' => Bitrix24Integration::API_CRM_LIST_COMPANY,
        ];
        $events = $result['event.offline.get']['events'] ?? [];
        $args = [];
        foreach ($events as $event) {
            $eventData = $event['EVENT_DATA'];
            if (isset($eventActionsDelete[$event['EVENT_NAME']])) {
                $id = array_values($eventData['FIELDS']);
                // Удаляем из локальной БД – но мы больше не синхронизируем, можно оставить пустым
                // ConnectorDb::invoke(ConnectorDb::FUNC_DELETE_CONTACT_DATA, [$eventActionsDelete[$event['EVENT_NAME']], $id],false);
            }
            if (isset($eventActionsUpdate[$event['EVENT_NAME']])){
                $arIds = array_values($eventData['FIELDS']);
                // Не синхронизируем – закомментировано
                // $args[] = $this->b24->crmListEnt($eventActionsUpdate[$event['EVENT_NAME']], $arIds);
                // foreach ($arIds as $id){ ... }
            }
            $this->b24->handleEvent([ 'event' => $event, 'data'  => $eventData]);
        }
        if(!empty($args)){
            $this->q_req = array_merge($this->q_req, array_merge(...$args));
        }
    }

    private function chunkAssociativeArray(array $array):array
    {
        $chunks = [];
        $chunk = [];
        $count = 0;
        foreach ($array as $key => $value) {
            if ($count >= 49) {
                $chunks[] = $chunk;
                $chunk = [];
                $count = 0;
            }
            $chunk[$key] = $value;
            $count++;
        }
        if ($count > 0) {
            $chunks[] = $chunk;
        }
        return $chunks;
    }

    /**
     * Синхронизация справочников – полностью отключена.
     */
    private function syncProcContacts()
    {
        // Закомментировано – больше не используем
        return;
    }

    public function executeTasks(): void
    {
        $this->insideExecuteTasks = true;
        try {
            $this->executeTasksInner();
        } finally {
            $this->insideExecuteTasks = false;
        }
    }

    private function executeTasksInner(): void
    {
        if ($this->needRestart) return;
        $this->b24->mainLogger->rotate();

        $delta = time() - $this->last_update_inner_num;
        if ($delta > 10) {
            $this->processState = 'updateSettings';
            $this->b24->checkNeedUpdateToken();
            if ($this->b24->getAuthFailureCount() >= Bitrix24Integration::AUTH_FAILURE_THRESHOLD) {
                $this->b24->mainLogger->writeError(
                    'Auth failure threshold reached (' . $this->b24->getAuthFailureCount() . '), restarting worker'
                );
                $this->needRestart = true;
                return;
            }
            $this->b24->b24GetPhones();
            $this->b24->updateSettings();
            $this->last_update_inner_num = time();
            $this->checkActiveChannels();
            // $this->syncProcContacts(); // ОТКЛЮЧЕНО
            $this->processState = 'idle';
            if ($this->needRestart) return;
        }

        $this->need_get_events = !$this->need_get_events;

        $this->drainPerCallQueues();

        if ($this->need_get_events) {
            $arg = $this->b24->eventOfflineGet();
            $this->q_req = array_merge($this->q_req, $arg);
        }
        if (count($this->q_req) > 0) {
            $this->processState = 'sendBatch';
            $chunks = $this->chunkAssociativeArray($this->q_req);
            $finalResult = [];
            foreach ($chunks as $chunk) {
                if ($this->needRestart) break;
                $response = $this->b24->sendBatch($chunk);
                $finalResult[] = $response['result']['result'] ?? [];
            }
            $this->processState = 'postProcessing';
            $result = array_merge(...$finalResult);
            $this->q_req = [];
            $this->postReceivingResponseProcessing($result);
            $this->handleEvent($result);
            $this->drainPerCallQueues();
            $this->processState = 'idle';
        }

        $this->hasPendingEvents = false;
        foreach ($this->perCallQueues as $queue) {
            if (!$queue->isEmpty()) {
                $this->hasPendingEvents = true;
                break;
            }
        }
    }

    private function drainPerCallQueues(): void
    {
        foreach ($this->perCallQueues as $queue) {
            while (!$queue->isEmpty()) {
                $event = $queue->bottom();
                if ($this->shouldDeferForPreAction($event)) {
                    break;
                }
                $queue->dequeue();
                $this->addDataToQueue($event);
            }
        }
    }

    public function postReceivingResponseProcessing(array $result): void
    {
        $tmpArr = [];
        foreach ($result as $key => $partResponse) {
            $id = '';
            $tube = '';
            $keyData = explode('_', $key);
            if(count($keyData) === 3){
                [$actionName, $id, $tube] = $keyData;
            }elseif(count($keyData) === 2){
                [$actionName, $id] = $keyData;
            }else{
                $actionName = $key;
            }

            if ($actionName === Bitrix24Integration::API_CALL_REGISTER) {
                $resultRegister = $this->b24->telephonyExternalCallPostRegister($key, $partResponse);
                if(!empty($resultRegister)){
                    [$linkedId, $callId] = $resultRegister;
                    $this->b24->mainLogger->writeInfo("Update call_id for $linkedId - $callId");
                    $this->tmpCallsData[$linkedId]['CALL_ID'] = $callId;
                }else{
                    $this->b24->mainLogger->writeInfo("fail Update call_id for $key");
                }
            // ========== ОТКЛЮЧЕНА ОБРАБОТКА СПРАВОЧНИКОВ ==========
            // } elseif (in_array($actionName,[Bitrix24Integration::API_CRM_CONTACT_COMPANY,Bitrix24Integration::API_CRM_COMPANY_CONTACT], true)) {
            //     ConnectorDb::invoke(ConnectorDb::FUNC_UPDATE_LINKS, [[$key => $partResponse]], false);
            // } elseif (in_array($id,['init', 'update'], true)){
            //     $this->b24->crmListEntResults($actionName, $id, $partResponse);
            // ====================================================
            } elseif(stripos($tube, Bitrix24Integration::B24_INVOKE_REST_CHANNEL) !== false){
                $this->invokeRestCheckResponse($key, $tube, $partResponse);
            } elseif ($actionName === Bitrix24Integration::API_ATTACH_RECORD) {
                $uploadUrl = $partResponse["uploadUrl"] ?? '';
                $data = $this->b24->telephonyPostAttachRecord($key, $uploadUrl);
                if (!empty($data)) {
                    $this->queueAgent->publish(json_encode($data, JSON_UNESCAPED_SLASHES), UploaderB24::B24_UPLOADER_CHANNEL);
                }
            } elseif ($actionName === Bitrix24Integration::API_CRM_ADD_LEAD) {
                $this->postCrmAddLead($key, $partResponse);
            } elseif ($actionName === Bitrix24Integration::API_CRM_ADD_CONTACT) {
                $this->postCrmAddContact($key, $partResponse);
            } elseif ($actionName === Bitrix24Integration::API_CALL_FINISH) {
                $this->b24->telephonyExternalCallPostFinish($key, $partResponse, $tmpArr);
            }
        }
        $tmpArr = array_merge(...$tmpArr);
        $this->q_req = array_merge($this->q_req, $tmpArr);
    }

    public function postCrmAddLead(string $key, $response): void
    {
        $key = explode('_', $key)[1]??'';
        if (!isset($this->tmpCallsData[$key])) {
            return;
        }
        $this->tmpCallsData[$key]['lead'] = 1;
        $this->tmpCallsData[$key]['wait'] = false;
        $this->tmpCallsData[$key]['crm-data']['CRM_ENTITY_TYPE'] = 'LEAD';
        $this->tmpCallsData[$key]['crm-data']['CRM_ENTITY_ID']   = $response;
    }

    public function postCrmAddContact(string $key, $response): void
    {
        $key = explode('_', $key)[1]??'';
        if (!isset($this->tmpCallsData[$key])) {
            return;
        }
        $this->tmpCallsData[$key]['lead'] = 1;
        $this->tmpCallsData[$key]['wait'] = false;
    }

    private function checkActiveChannels():void
    {
        if(!$this->searchEntities){
            return;
        }
        $am = Util::getAstManager();
        $channels = $am->GetChannels();
        foreach ($this->tmpCallsData as $linkedid => $data){
            if(!isset($channels[$linkedid])){
                if (isset($this->perCallQueues[$linkedid])
                    && !$this->perCallQueues[$linkedid]->isEmpty()) {
                    continue;
                }
                $cleanTime = $this->tmpCallsData[$linkedid]['cleanTime'] ?? 0;
                if ($cleanTime === 0) {
                    $this->tmpCallsData[$linkedid]['cleanTime'] = time();
                    $this->b24->mainLogger->writeInfo("Clearing the event queue wait 120s. $linkedid");
                    continue;
                }
                if ((time() - $cleanTime) < 120) {
                    continue;
                }
                unset($this->tmpCallsData[$linkedid]);
                unset($this->perCallQueues[$linkedid]);
                $this->b24->mainLogger->writeInfo("Clearing the event queue. All channels are completed $linkedid");
            }
        }
    }

    private function parseInnerNumbers(string $data):array
    {
        $result = [];
        preg_match_all('/\[(\d{2,},?)+\]/m', $data, $matches, PREG_SET_ORDER);
        if(!empty($matches)){
            $result = explode(',', str_replace(['[',']'], ['', ''], $matches[0][0]??''));
        }
        return $result;
    }

}

// Start worker process
if(isset($argv) && count($argv) !== 1) {
    WorkerBitrix24IntegrationHTTP::startWorker($argv??[]);
}
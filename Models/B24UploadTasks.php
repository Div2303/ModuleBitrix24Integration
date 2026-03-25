<?php
namespace Modules\ModuleBitrix24Integration\Models;

use MikoPBX\Modules\Models\ModulesModelsBase;

/**
 * Class B24UploadTasks
 *
 * @package Modules\ModuleBitrix24Integration\Models
 */
class B24UploadTasks extends ModulesModelsBase
{
    public const STATUS_PENDING = 'pending';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_FAILED = 'failed';

    /**
     * @Primary
     * @Identity
     * @Column(type="integer", nullable=false)
     */
    public $id;

    /**
     * @Column(type="string", nullable=true)
     */
    public $call_id;

    /**
     * @Column(type="string", nullable=false)
     */
    public $filename;

    /**
     * @Column(type="string", nullable=false)
     */
    public $upload_url;

    /**
     * @Column(type="integer", nullable=false, default=0)
     */
    public $attempts = 0;

    /**
     * @Column(type="integer", nullable=false)
     */
    public $last_attempt;

    /**
     * @Column(type="integer", nullable=false)
     */
    public $next_attempt;

    /**
     * @Column(type="string", nullable=false, default="pending")
     */
    public $status = self::STATUS_PENDING;

    /**
     * @Column(type="string", nullable=true)
     */
    public $error_message;

    /**
     * @Column(type="integer", nullable=false)
     */
    public $created_at;

    /**
     * @Column(type="integer", nullable=false)
     */
    public $updated_at;

    public function initialize(): void
    {
        $this->setSource('b24_upload_tasks');
        parent::initialize();
        $this->useDynamicUpdate(true);
    }
}
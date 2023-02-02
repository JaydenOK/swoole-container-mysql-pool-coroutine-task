<?php

namespace module\task;

use module\lib\MysqliClient;

abstract class TaskModel implements Task
{

    /**
     * @var MysqliClient
     */
    protected $mysqliClient;
    /**
     * @var \module\lib\MysqliDb
     */
    protected $query;

    /**
     * TaskModel constructor.
     * @param \PDO | \Swoole\Database\PDOProxy | null $poolObject
     */
    public function __construct($poolObject = null)
    {
        $this->mysqliClient = new MysqliClient();
        $this->query = $this->mysqliClient->getQuery();
    }

    //关闭mysql短连接
    public function __destruct()
    {
        $this->query->disconnect();
        $this->query = null;
        $this->mysqliClient = null;
    }

}
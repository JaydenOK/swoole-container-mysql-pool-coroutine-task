<?php

namespace module\task;

use module\lib\MysqliClient;

abstract class TaskModel implements Task
{

    /**
     * @var MysqliClient
     */
    protected $mysqlClient;
    /**
     * @var \module\lib\MysqliDb
     */
    protected $query;
    /**
     * @var bool
     */
    protected $isUsePool = false;
    /**
     * @var \mysqli
     */
    protected $poolObject;

    /**
     * TaskModel constructor.
     * @param null $poolObject
     */
    public function __construct($poolObject = null)
    {
        if ($poolObject !== null) {
            $this->isUsePool = true;
            $this->poolObject = $poolObject;
        } else {
            $this->mysqlClient = new MysqliClient();
            $this->query = $this->mysqlClient->getQuery();
        }
    }

    //关闭mysql短连接
    public function __destruct()
    {
        if ($this->isUsePool) {
            //不断开连接，直接归还连接池
//            if (method_exists($this->poolObject, 'close')) {
//                $this->poolObject->close();   //mysqli
//            }

//            $this->poolObject = null;     //pdo
        } else {
            //MysqliDb
            if (method_exists($this->query, 'disconnect')) {
                $this->query->disconnect();
            }
            //\mysqli
            if (method_exists($this->query, 'close')) {
                $this->query->close();
            }
            $this->query = null;
            $this->mysqlClient = null;
        }
    }

}
<?php
/**
 * Coroutine协程容器并发实例，适用于内部系统要处理大量耗时的任务，直接命令行执行，每个协程单独mysql连接(支持连接池)
 *
 * @author https://github.com/JaydenOK
 */

namespace module\server;

use Exception;
use InvalidArgumentException;
use module\lib\MysqliClient;
use module\task\TaskFactory;
use Swoole\ConnectionPool;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\WaitGroup;
use function Swoole\Coroutine\Run;

class TaskManager
{

    /**
     * @var string
     */
    private $taskType;
    /**
     * @var string
     */
    private $pidFile;
    /**
     * @var int
     */
    private $concurrencyNum;
    /**
     * @var int
     */
    private $maxTaskNum;
    /**
     * @var int|null
     */
    private $id;
    /**
     * @var bool 是否使用连接池
     */
    private $isUsePool = false;
    /**
     * @var int 连接池大小
     */
    private $poolSize = 50;
    /**
     * @var ConnectionPool
     */
    private $pool;

    public function run($argv)
    {
        try {
            $this->taskType = isset($argv[1]) ? (string)$argv[1] : '';
            $this->concurrencyNum = isset($argv[2]) ? (int)$argv[2] : 1;
            $this->maxTaskNum = isset($argv[3]) ? (int)$argv[3] : 50;
            if (isset($argv[4])) {
                $this->id = (int)$argv[4];
            }
            if (isset($argv[5])) {
                $this->isUsePool = (bool)$argv[5];
            }
            if (isset($argv[6])) {
                $this->poolSize = (int)$argv[6];
            }
            if (empty($this->taskType) || $this->concurrencyNum < 1 || $this->maxTaskNum < 1) {
                throw new InvalidArgumentException('params error');
            }
            $this->pidFile = $this->taskType . '.pid';
            if (!in_array($this->taskType, TaskFactory::taskList())) {
                throw new InvalidArgumentException('task_type not exist');
            }
            $this->runTask();
        } catch (Exception $e) {
            $this->logMessage('Exception:' . $e->getMessage());
        }
        echo 'done' . PHP_EOL;
    }

    //命令行协程执行，控制并发
    private function runTask()
    {
        $startTime = time();
        //一键协程化，被`Hook`的函数需要在[协程容器]中使用
        //Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL);
        //所有的协程必须在协程容器里面创建，Swoole 程序启动的时候大部分情况会自动创建协程容器，其他直接裸写协程的方式启动程序，需要先创建一个协程容器 (Coroutine\run() 函数
        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]); //不包括CURL， v4.4+版本使用此方法。从 v4.5.4 版本起，`SWOOLE_HOOK_ALL` 包括 `SWOOLE_HOOK_CURL`
        Run(function () {
            $this->checkPool();
            $taskModel = TaskFactory::factory($this->taskType, $this->getPoolObject());
            $taskLists = $taskModel->getTaskList(['limit' => $this->maxTaskNum, 'id' => $this->id]);
            if (empty($taskLists)) {
                echo 'not task wait' . PHP_EOL;
                return;
            }
            $count = count($taskLists);
            $this->logMessage('total task num:' . $count);
            //有限通道channel阻塞，控制并发数
            $chan = new Channel($this->concurrencyNum);
            //协程依赖同步等待
            $wg = new WaitGroup();
            $wg->add($count);
            foreach ($taskLists as $task) {
                //阻塞
                $chan->push(1);
                $this->logMessage('push task');
                go(function () use ($wg, $chan, $task) {
                    try {
                        //每个协程，独立连接（可连接池）
                        $taskModel = TaskFactory::factory($this->taskType, $this->getPoolObject());
                        $result = $taskModel->taskRun($task);
                        $this->logMessage('taskDone:' . $result);
                    } catch (Exception $e) {
                        $this->logMessage('taskRun Exception:' . $e->getMessage());
                    }
                    $taskModel = null;
                    //完成减少计数，解除阻塞
                    $wg->done();
                    $chan->pop();
                });
            }
            // 主协程等待，挂起当前协程，等待所有任务完成后恢复当前协程的执行
            $wg->wait();
            $this->logMessage("all {$this->taskType} taskDone: {$count}");
        });
        $this->logMessage('done, use time[s]:' . (time() - $startTime));
    }

    private function logMessage($logData)
    {
        $logFile = MODULE_DIR . '/logs/server-' . date('Y-m') . '.log';
        $logData = (is_array($logData) || is_object($logData)) ? json_encode($logData, JSON_UNESCAPED_UNICODE) : $logData;
        file_put_contents($logFile, date('[Y-m-d H:i:s]') . $logData . PHP_EOL, FILE_APPEND);
    }

    /**
     * @return bool
     * @throws Exception
     */
    private function checkPool()
    {
        if ($this->isUsePool) {
            $this->logMessage('init pool, size:' . $this->poolSize);
            $this->pool = (new MysqliClient())->initPool($this->poolSize);
            //$this->pool = (new PdoClient())->initPool($this->poolSize);
            //预热，填充连接池
            $this->pool->fill();
            defer(function () {
                $this->closePool();
            });
            return true;
        }
        return false;
    }

    /**
     * @return mixed|null
     * @throws Exception
     */
    private function getPoolObject()
    {
        if (!$this->isUsePool) {
            return null;
        }
        $poolObject = $this->pool->get();
        if ($poolObject === null) {
            throw new Exception('getNullPoolObject');
        }
        $this->logMessage('pool get:' . spl_object_hash($poolObject));
        defer(function () use ($poolObject) {
            //协程结束归还对象
            if ($poolObject !== null) {
                $this->logMessage('pool put:' . spl_object_hash($poolObject));
                $this->pool->put($poolObject);
            }
        });
        return $poolObject;
    }

    /**
     * @return bool
     */
    private function closePool()
    {
        if (!$this->isUsePool || $this->pool === null) {
            return false;
        }
        $this->logMessage('close pool');
        $this->pool->close();
        $this->pool = null;
        return true;
    }


}
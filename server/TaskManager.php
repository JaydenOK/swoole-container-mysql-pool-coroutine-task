<?php

namespace module\server;

use chan;
use Exception;
use InvalidArgumentException;
use module\lib\PdoPoolClient;
use module\task\TaskFactory;
use PDO;
use Swoole\Coroutine;
use Swoole\Database\PDOPool;
use Swoole\Database\PDOProxy;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\Process;
use Swoole\Server;
use Swoole\Table;
use Swoole\Timer;

class TaskManager
{
    const EVENT_START = 'start';
    const EVENT_MANAGER_START = 'managerStart';
    const EVENT_WORKER_START = 'workerStart';
    const EVENT_WORKER_STOP = 'workerStop';
    const EVENT_REQUEST = 'request';

    /**
     * @var \Swoole\Http\Server
     */
    protected $httpServer;
    /**
     * @var string
     */
    private $taskType;
    /**
     * @var int|string
     */
    private $port;
    private $processPrefix = 'co-task-';
    private $setting = ['worker_num' => 2, 'enable_coroutine' => true];
    /**
     * @var bool
     */
    private $daemon;
    /**
     * @var string
     */
    private $pidFile;
    /**
     * @var int
     */
    private $poolSize = 16;
    /**
     * 是否使用连接池，可参数指定，默认不使用
     * @var bool
     */
    private $isUsePool = false;
    /**
     * @var PDOPool
     */
    private $pool;
    private $checkAvailableTime = 1;
    private $checkLiveTime = 10;
    private $availableTimerId;
    private $liveTimerId;
    /**
     * @var Table
     */
    private $poolTable;
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

    public function run($argv)
    {
        try {
            $this->taskType = isset($argv[1]) ? (string)$argv[1] : '';
            $this->concurrencyNum = isset($argv[2]) ? (int)$argv[2] : 1;
            $this->maxTaskNum = isset($argv[3]) ? (int)$argv[3] : 50;
            $this->id = isset($argv[4]) ? (int)$argv[4] : null;
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
    }

    //命令行协程执行，控制并发
    private function runTask()
    {
        //一键协程化，被`Hook`的函数需要在[协程容器]中使用
        //Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL);
        //所有的协程必须在协程容器里面创建，Swoole 程序启动的时候大部分情况会自动创建协程容器，其他直接裸写协程的方式启动程序，需要先创建一个协程容器 (Coroutine\run() 函数
        \Swoole\Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]); //不包括CURL， v4.4+版本使用此方法。从 v4.5.4 版本起，`SWOOLE_HOOK_ALL` 包括 `SWOOLE_HOOK_CURL`
        \Swoole\Coroutine\Run(function () {
            $taskModel = TaskFactory::factory($this->taskType);
            $lists = $taskModel->getTaskList(['limit' => $this->maxTaskNum, 'id' => $this->id]);
            if (empty($lists)) {
                return 'not task wait';
            }
            echo date('[Y-m-d H:i:s]') . 'total:' . count($lists) . print_r(array_column($lists, 'id'), true) . PHP_EOL;
            //有限通道channel阻塞，控制并发数
            $chan = new Coroutine\Channel($this->concurrencyNum);
            //协程依赖同步等待
            $wg = new \Swoole\Coroutine\WaitGroup();
            $wg->add(count($lists));
            foreach ($lists as $task) {
                //阻塞
                $chan->push(1);
                $this->logMessage('task running');
                go(function () use ($wg, $chan, $task) {
                    try {
                        $taskModel = TaskFactory::factory($this->taskType);
                        $result = $taskModel->taskRun($task);
                        $this->logMessage('taskDone' . $result);
                        $taskModel = null;
                    } catch (Exception $e) {
                        $e->getMessage();
                        $this->logMessage('taskRun Exception:' . $e->getMessage());
                    }
                    //完成减少计数，解除阻塞
                    $wg->done();
                    $chan->pop();
                });
            }
            // 主协程等待，挂起当前协程，等待所有任务完成后恢复当前协程的执行
            $wg->wait();
            $this->logMessage('all task done:' . $this->taskType);
        });
        $this->logMessage('done');
    }

    public function onRequest(Request $request, Response $response)
    {
        try {
            $concurrency = isset($request->get['concurrency']) ? (int)$request->get['concurrency'] : 5;  //并发数
            $total = isset($request->get['total']) ? (int)$request->get['total'] : 100;  //需总处理记录数
            $taskType = isset($request->get['task_type']) ? (string)$request->get['task_type'] : '';  //任务类型
            if ($concurrency <= 0 || empty($taskType)) {
                throw new InvalidArgumentException('parameters error');
            }
            //数据库配置信息
            $pdo = $this->isUsePool ? $this->getPoolObject() : null;
            $mainTaskModel = TaskFactory::factory($taskType, $pdo);
            $taskList = $mainTaskModel->getTaskList(['limit' => $total]);       //已一键协程化，多个请求时，此处不阻塞
            if (empty($taskList)) {
                throw new InvalidArgumentException('no tasks waiting to be executed');
            }
            $taskCount = count($taskList);
            $startTime = time();
            $this->logMessage("task count:{$taskCount}");
            $taskChan = new chan($taskCount);
            //初始化并发数量
            $producerChan = new chan($concurrency);
            $dataChan = new chan($total);
            for ($size = 1; $size <= $concurrency; $size++) {
                $producerChan->push(1);
            }
            foreach ($taskList as $task) {
                //增加当前任务类型标识
                $task = array_merge($task, ['task_type' => $taskType]);
                $taskChan->push($task);
            }
            //创建生产者主协程，用于投递任务
            go(function () use ($taskChan, $producerChan, $dataChan) {
                while (true) {
                    $chanStatsArr = $taskChan->stats(); //queue_num 通道中的元素数量
                    if (!isset($chanStatsArr['queue_num']) || $chanStatsArr['queue_num'] == 0) {
                        //queue_num 通道中的元素数量
                        $this->logMessage('finish deliver');
                        break;
                    }
                    //阻塞获取
                    $producerChan->pop();
                    $task = $taskChan->pop();
                    //创建子协程，执行任务，使用channel传递数据
                    go(function () use ($producerChan, $dataChan, $task) {
                        try {
                            //每个协程，创建独立连接（可从连接池获取）
                            $pdo = $this->isUsePool ? $this->getPoolObject() : null;
                            $taskModel = TaskFactory::factory($task['task_type'], $pdo);
                            Coroutine::defer(function () use ($taskModel) {
                                //释放内存及mysql连接
                                unset($taskModel);
                            });
                            $this->logMessage('taskRun:' . $task['id']);
                            $responseBody = $taskModel->taskRun($task['id'], $task);
                            $this->logMessage("taskFinish:{$task['id']}");
                        } catch (Exception $e) {
                            $this->logMessage("taskRunException: id:{$task['id']}: msg:" . $e->getMessage());
                            $responseBody = null;
                        }
                        $pushStatus = $dataChan->push(['id' => $task['id'], 'data' => $responseBody]);
                        if ($pushStatus !== true) {
                            $this->logMessage('push errCode:' . $dataChan->errCode);
                        }
                        //处理完，恢复producerChan协程
                        $producerChan->push(1);
                    });
                }
            });
            //消费数据
            for ($i = 1; $i <= $taskCount; $i++) {
                //阻塞，等待投递结果, 通道被关闭时，执行失败返回 false,
                $receiveData = $dataChan->pop();
                if ($receiveData === false) {
                    $this->logMessage('channel close, pop errCode:' . $dataChan->errCode);
                    //退出
                    break;
                }
                $this->logMessage('taskDone:' . $receiveData['id']);
                $mainTaskModel->taskDone($receiveData['id'], $receiveData['data']);
            }
            //返回响应
            $endTime = time();
            $return = ['taskCount' => $taskCount, 'concurrency' => $concurrency, 'useTime' => ($endTime - $startTime) . 's'];
        } catch (InvalidArgumentException $e) {
            $return = json_encode(['Exception' => $e->getMessage()]);
        } catch (Exception $e) {
            $this->logMessage('Exception:' . $e->getMessage());
            $return = json_encode(['Exception' => $e->getMessage()]);
        }
        $mainTaskModel = null;
        return $response->end(json_encode($return));
    }

    private function logMessage($logData)
    {
        $logData = (is_array($logData) || is_object($logData)) ? json_encode($logData, JSON_UNESCAPED_UNICODE) : $logData;
        echo date('[Y-m-d H:i:s]') . $logData . PHP_EOL;
    }

    private function status()
    {
        $pidFile = MODULE_DIR . '/logs/' . $this->pidFile;
        if (!file_exists($pidFile)) {
            throw new Exception('server not running');
        }
        $pid = file_get_contents($pidFile);
        //$signo=0，可以检测进程是否存在，不会发送信号
        if (!Process::kill($pid, 0)) {
            echo 'not running, pid:' . $pid . PHP_EOL;
        } else {
            echo 'running, pid:' . $pid . PHP_EOL;
        }
    }

}
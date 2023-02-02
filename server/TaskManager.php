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
    private $concurrency;
    /**
     * @var int
     */
    private $maxTaskNum;

    public function run($argv)
    {
        try {
            $this->taskType = isset($argv[1]) ? (string)$argv[1] : '';
            $this->concurrency = isset($argv[2]) ? (int)$argv[2] : 2;
            $this->maxTaskNum = isset($argv[3]) ? (int)$argv[3] : 50;
            if (empty($this->taskType)) {
                throw new InvalidArgumentException('task_type error');
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
            $total = isset($params['limit']) && !empty($params['total']) ? (int)$params['total'] : 100;
            $id = isset($params['id']) && !empty($params['id']) ? $params['id'] : '';
            $dbServerKey = 'db_server_yibai_master';
            $key = 'db_account_manage';
            $db = createDbConnection($dbServerKey, $key);
            //查询出要处理的记录
            $lists = $db->where('id<', 1000)->limit($total)->get('yibai_amazon_account')->result_array();
            if (empty($lists)) {
                return 'not task wait';
            }
            echo date('[Y-m-d H:i:s]') . 'total:' . count($lists) . print_r(array_column($lists, 'id'), true) . PHP_EOL;
            $batchRunNum = 10;
            $batchAccountList = array_chunk($lists, $batchRunNum);
            foreach ($batchAccountList as $key => $accountList) {
                $result = [];
                //分批次执行
                $wg = new Swoole\Coroutine\WaitGroup();
                foreach ($accountList as $account) {
                    echo date('[Y-m-d H:i:s]') . "id: {$account['id']},running" . PHP_EOL;
                    // 增加计数
                    $wg->add();
                    //父子协程优先级
                    //优先执行子协程 (即 go() 里面的逻辑)，直到发生协程 yield(co::sleep 处)，然后协程调度到外层协程
                    go(function () use ($wg, &$result, $account) {
                        //启动一个协程客户端client
                        $cli = new Swoole\Coroutine\Http\Client('api.amazon.com', 443, true);
                        $cli->setHeaders([
                            'Host' => 'api.amazon.com',
                            'User-Agent' => 'Chrome/49.0.2587.3',
                            'Accept' => 'text/html,application/xhtml+xml,application/xml',
                            'Accept-Encoding' => 'gzip',
                        ]);
                        $cli->set(['timeout' => 1]);
                        $cli->setMethod('POST');
                        $cli->get('/auth/o2/token');
                        $result[] = $cli->body;
                        $cli->close();
                        //完成减少计数
                        $wg->done();
                    });
                }
                // 主协程等待，挂起当前协程，等待所有任务完成后恢复当前协程的执行
                $wg->wait();
                foreach ($result as $item) {
                    $id = $item['id'];
                    echo date('[Y-m-d H:i:s]') . "id: {$id} done,result:" . json_encode($item, 256) . PHP_EOL;
                }
            }
        });
        return 'finish';
    }

    /**
     * 当前进程重命名
     * @param $processName
     * @return bool|mixed
     */
    private function renameProcessName($processName)
    {
        if (function_exists('cli_set_process_title')) {
            return cli_set_process_title($processName);
        } else if (function_exists('swoole_set_process_name')) {
            return swoole_set_process_name($processName);
        }
        return false;
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
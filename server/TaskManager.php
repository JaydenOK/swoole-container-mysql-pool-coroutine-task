<?php
/**
 * Coroutine协程容器并发实例，适用于内部系统要处理大量耗时的任务，直接命令行执行，每个协程单独mysql连接
 */

namespace module\server;

use Exception;
use InvalidArgumentException;
use module\task\TaskFactory;
use Swoole\Coroutine;
use Swoole\Process;

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
        $startTime = time();
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
            $count = count($lists);
            $this->logMessage('total task num:' . $count);
            //有限通道channel阻塞，控制并发数
            $chan = new Coroutine\Channel($this->concurrencyNum);
            //协程依赖同步等待
            $wg = new \Swoole\Coroutine\WaitGroup();
            $wg->add($count);
            foreach ($lists as $task) {
                //阻塞
                $chan->push(1);
                $this->logMessage('push task');
                go(function () use ($wg, $chan, $task) {
                    try {
                        //每个协程，独立连接，可改用连接池
                        $taskModel = TaskFactory::factory($this->taskType);
                        $result = $taskModel->taskRun($task);
                        $this->logMessage('taskDone:' . $result);
                    } catch (Exception $e) {
                        $e->getMessage();
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
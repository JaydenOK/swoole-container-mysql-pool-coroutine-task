## swoole-coroutine-mysql-pdo-pool-task
Coroutine协程容器并发实例，适用于内部系统要处理大量耗时的任务，直接命令行执行，每个协程单独mysql连接


#### 功能逻辑
```text
在协程容器里直接执行
一键协程化mysql连接
通道Channel控制并发数
WaitGroup等待
多类型，多任务执行
task目录下Model为每个类型任务模块，增加新模块继承TaskModel（实现了Task接口）即可
```

#### 版本
- PHP 7.1
- Swoole 4.5.11


#### 测试结果

```shell script

示例: [root@ac_web ]# php service.php {taskType} {concurrencyNum} {maxTaskNum}


[root@ac_web ]# php service.php Amazon 20 500


```
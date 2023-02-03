## swoole-container-mysql-pool-coroutine-task
swoole协程容器并发任务项目，多类型多任务，适用于内部系统要处理大量耗时的任务（支持连接池）
## 

- 另一种实现方案，请前往 [协程并发任务服务项目](https://github.com/JaydenOK/swoole-mysql-pool-coroutine-server-task).


#### 功能逻辑
```text
在协程容器里直接执行
一键协程化mysql连接
通道Channel控制并发数
WaitGroup等待
多类型，多任务执行
task目录下Model为每个类型任务模块，增加新模块继承TaskModel（实现了Task接口）即可
支持连接池
```

#### 版本
- PHP 7.1
- Swoole 4.5.11


#### 测试结果

```shell script

示例: [root@ac_web ]# php service.php {taskType} {concurrencyNum} {maxTaskNum} {id} {isUsePool} {poolSize}


[root@ac_web ]# [root@ac_web pdo_coroutine_task]# php service.php Amazon 40 300 0 1 50


```
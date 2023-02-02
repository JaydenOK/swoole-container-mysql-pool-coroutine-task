<?php
/**
 * Coroutine协程并发实例，适用于内部系统要处理大量耗时的任务
 *
 */

error_reporting(-1);
ini_set('display_errors', 1);

require 'bootstrap.php';

$manager = new module\server\TaskManager();
$manager->run($argv);

<?php

namespace module\task;

class ShopeeModel extends TaskModel
{

    public function tableName()
    {
        return 'yibai_shopee_account';
    }

    public function getTaskList($params)
    {
        // TODO: Implement getTaskList() method.
        if ($this->isUsePool) {
            $sql = "select * from {$this->tableName()} limit {$params['limit']}";
            $queryResult = $this->poolObject->query($sql);
            $result = [];
            while ($row = $queryResult->fetch_assoc()) {
                $result[] = $row;
            }
        } else {
            $result = $this->query->page(1)->limit($params['limit'])->paginate($this->tableName());
        }
        return $result;
    }

    /**
     * 重新解压，编译支持https
     * phpize && ./configure --enable-openssl --enable-http2 && make && sudo make install
     * @param $task
     * @return mixed
     * @throws \Exception
     */
    public function taskRun($task)
    {
        // TODO: Implement taskRun() method.
        if ($this->isUsePool) {
            $randNum = mt_rand(0, 10);
            $sql = "update {$this->tableName()} set refresh_num={$randNum} where id={$task['id']}";
            $res = $this->poolObject->query($sql);

            $host = 'partner.shopeemobile.com';
            $timestamp = time();
            $path = '/api/v2/auth/access_token/get';
            $sign = '111';
            $data = [];
            $data['partner_id'] = 111;
            $data['refresh_token'] = '222';
            $data['merchant_id'] = 333;
            $path .= '?timestamp=' . $timestamp . '&sign=' . $sign . '&partner_id=' . $data['partner_id'];
            $cli = new \Swoole\Coroutine\Http\Client($host, 443, true);
            $cli->set(['timeout' => 10]);
            $cli->setHeaders([
                'Host' => $host,
                'Content-Type' => 'application/json;charset=UTF-8',
            ]);
            $data = [];
            $cli->post($path, json_encode($data));
            $responseBody = $cli->body;
            //处理请求返回数据
            $refreshMsg = json_encode($responseBody, 256);
            $refreshTime = date('Y-m-d H:i:s');
            $sql = "update {$this->tableName()} set refresh_msg='{$refreshMsg}', refresh_time='{$refreshTime}' where id={$task['id']}";
            $this->poolObject->query($sql);
            $res = $this->poolObject->affected_rows;
        } else {
            //todo 模拟业务耗时处理逻辑
            // TODO: Implement taskRun() method.
            $data = ['refresh_num' => mt_rand(0, 10)];
            $res = $this->query->where('id', $task['id'])->update($this->tableName(), $data);
            $host = 'partner.shopeemobile.com';
            $timestamp = time();
            $path = '/api/v2/auth/access_token/get';
            $sign = '111';
            $data = [];
            $data['partner_id'] = 111;
            $data['refresh_token'] = '222';
            $data['merchant_id'] = 333;
            $path .= '?timestamp=' . $timestamp . '&sign=' . $sign . '&partner_id=' . $data['partner_id'];
            $cli = new \Swoole\Coroutine\Http\Client($host, 443, true);
            $cli->set(['timeout' => 10]);
            $cli->setHeaders([
                'Host' => $host,
                'Content-Type' => 'application/json;charset=UTF-8',
            ]);
            $data = [];
            $cli->post($path, json_encode($data));
            $responseBody = $cli->body;
            //处理请求返回数据
            $data = ['refresh_msg' => json_encode($responseBody, 256), 'refresh_time' => date('Y-m-d H:i:s')];
            $res = $this->query->where('id', $task['id'])->update($this->tableName(), $data);
        }
        return $res;
    }

}
package com.laiyy.study.rocketmqprovider.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author laiyy
 * @date 2019/4/21 18:03
 * @description
 */
public class TransactionListenerImpl implements TransactionListener {

    /**
     * 存储对应书屋的状态信息， key：事务id，value：事务执行的状态
     */
    private ConcurrentMap<String, Integer> maps = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     *
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        // 事务id
        String transactionId = message.getTransactionId();

        // 0：执行中，状态未知
        // 1：本地事务执行成功
        // 2：本地事务执行失败

        maps.put(transactionId, 0);

        try {
            System.out.println("正在执行本地事务。。。。");
            // 模拟本地事务
            TimeUnit.SECONDS.sleep(65);
            System.out.println("本地事务执行成功。。。。");
            maps.put(transactionId, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            maps.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 消息回查
     *
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String transactionId = messageExt.getTransactionId();

        System.out.println("正在执行消息回查，事务id：" + transactionId);

        // 获取事务id的执行状态
        if (maps.containsKey(transactionId)) {
            int status = maps.get(transactionId);
            System.out.println("消息回查状态：" + status);
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                default:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
        return LocalTransactionState.UNKNOW;
    }
}

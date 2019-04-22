package com.laiyy.study.rocketmqprovider.transaction;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author laiyy
 * @date 2019/4/21 16:18
 * @description
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 1、创建 TransactionMQProducer
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer");

        // 2、设置 name server
        producer.setNamesrvAddr("192.168.52.200:9876");

        // 3、指定消息监听对象，用于执行本地事务和消息回查
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);

        // 4、线程池
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-thread");
                return thread;
            }
        });

        producer.setExecutorService(executor);

        // 5、开启 producer
        producer.start();

        // 6、创建消息
        Message message = new Message("TRANSACTION_TOPIC", "TAG_A", "KEYS_!", "HELLO！TRANSACTION!".getBytes(RemotingHelper.DEFAULT_CHARSET));

        // 7、发送消息
        TransactionSendResult result = producer.sendMessageInTransaction(message, "hello-transaction");

        System.out.println(result);

        // 关闭 producer
        producer.shutdown();
    }

}

package com.laiyy.study.rocketmqprovider.demo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author laiyy
 * @date 2019/4/21 16:18
 * @description
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 1、创建 DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo-producer");

        // 2、设置 name server
        producer.setNamesrvAddr("192.168.52.200:9876");

        // 3、开启 producer
        producer.start();

        // 创建消息
        Message message = new Message("TOPIC_DEMO", "TAG_A", "KEYS_!", "HELLO！".getBytes(RemotingHelper.DEFAULT_CHARSET));

        // 发送消息
        SendResult result = producer.send(message);
        System.out.println(result);
        // 关闭 producer
        producer.shutdown();
    }

}

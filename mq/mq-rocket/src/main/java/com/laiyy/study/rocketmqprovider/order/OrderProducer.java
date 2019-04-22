package com.laiyy.study.rocketmqprovider.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author laiyy
 * @date 2019/4/21 16:18
 * @description
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 1、创建 DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo-producer");

        // 2、设置 name server
        producer.setNamesrvAddr("192.168.52.200:9876");

        // 3、开启 producer
        producer.start();

        // 连续发送 5 条信息
        for (int index = 1; index <= 5; index++) {
            // 创建消息
            Message message = new Message("TOPIC_DEMO", "TAG_A", "KEYS_!", ("HELLO！" + index).getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 指定 MessageQueue，顺序发送消息
            // 第一个参数：消息体
            // 第二个参数：选中指定的消息队列对象（会将所有的消息队列传进来，需要自己选择）
            // 第三个参数：选择对应的队列下标
            SendResult result = producer.send(message, new MessageQueueSelector() {
                // 第一个参数：所有的消息队列对象
                // 第二个参数：消息体
                // 第三个参数：传入的消息队列下标
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    // 获取队列下标
                    int index = (int) o;
                    return list.get(index);
                }
            }, 0);
            System.out.println("发送第：" + index + " 条信息成功：" + result);
        }
        // 关闭 producer
        producer.shutdown();
    }

}

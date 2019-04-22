package com.laiyy.study.rocketmqprovider.boardcast;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author laiyy
 * @date 2019/4/21 17:18
 * @description
 */
public class Consumer1 {

    public static void main(String[] args) throws MQClientException {
        // 1、创建 DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("boardcast-consumer");

        // 2、设置 name server
        consumer.setNamesrvAddr("192.168.52.200:9876");

        // 设置消息拉取最大数
        consumer.setConsumeMessageBatchMaxSize(2);

        // 修改消费模式，默认是集群消费模式，修改为广播消费模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
//
        // 3、设置 subscribe
        consumer.subscribe("BOARD_CAST_TOPIC", // 要消费的主题
                "*" // 过滤规则
        );

        // 4、创建消息监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 5、获取消息信息
                for (MessageExt msg : list) {
                    // 获取主题
                    String topic = msg.getTopic();
                    // 获取标签
                    String tags = msg.getTags();
                    // 获取信息
                    try {
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("B  Consumer 消费信息：topic：" + topic+ "，tags：" + tags + "，消息体：" + result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                // 6、返回消息读取状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();


    }

}

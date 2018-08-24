package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author:wjy
 * @Date: 2018/8/24.
 */
public class ConsumerStart {

	public static void main(String[] args) {
		//消费者的组名
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("PushConsumer");

		//指定NameServer地址，多个地址以 ; 隔开
		consumer.setNamesrvAddr("140.143.244.92:9876");
		consumer.setConsumerGroup("");
		try {
			consumer.subscribe("TopicTest","TagA");

			//设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
			//如果非第一次启动，那么按照上次消费的位置继续消费
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

					try {
						for (MessageExt messageExt:list){
							//输出消息内容
							System.out.println("messageExt: " + messageExt);

							String messageBody = new String(messageExt.getBody(), "utf-8");
							//输出消息内容
							System.out.println("消费响应：Msg: " + messageExt.getMsgId() + ",msgBody: " + messageBody);
						}
					}catch (Exception e){
						//稍后再试
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
					//消费成功
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
}

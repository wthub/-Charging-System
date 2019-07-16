package consumers_producers;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者
 */
public class Consumers {
    private static final String topic = "one";
    private static final Integer threads = 2;

    public static void main(String[] args) {
        // 配置相应的属性
        Properties prop = new Properties();
        // 配置zk
        prop.put("zookeeper.connect","192.168.17.10:2181");
        // 配置消费者组
        prop.put("group.id","hz");
        // 将配置项加载
        ConsumerConfig config = new ConsumerConfig(prop);
        // 创建消费者
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        // 接收数据
        Map<String, Integer> hashMap = new HashMap<>();
        hashMap.put(topic,threads);
        // 创建获取信息流
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(hashMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = consumerMap.get(topic);
        // 循环取值
        for(KafkaStream<byte[], byte[]> kafkaStream:kafkaStreams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(MessageAndMetadata<byte[], byte[]> m :kafkaStream){
                        String s = new String(m.message());
                        System.out.println(s);
                    }
                }
            }).start();
        }
    }
}

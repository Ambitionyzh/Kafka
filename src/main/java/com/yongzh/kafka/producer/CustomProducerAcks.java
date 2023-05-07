package com.yongzh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author yongzh
 * @version 1.0
 * @program: Kafka
 * @description:
 * @date 2023/4/28 20:30
 */
public class CustomProducerAcks {
    public static void main(String[] args) {
        //0 配置
        Properties properties = new Properties();

        //连接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");

        //指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        //1.创建Kafka生产者对象
        KafkaProducer<Object, String> kafkaProducer = new KafkaProducer<>(properties);


        //2.发送数据

        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","wuhu"+i));
        }
        //3.关闭资源
        kafkaProducer.close();
    }
}

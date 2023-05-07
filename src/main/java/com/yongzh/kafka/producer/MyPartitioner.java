package com.yongzh.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author yongzh
 * @version 1.0
 * @program: Kafka
 * @description:
 * @date 2023/4/28 22:27
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        //获取数据
        String msgValue = value.toString();

        int partition;
        if(msgValue.contains("wuhu")){
            partition = 0;
        }else{
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

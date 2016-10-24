package com.abel.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import scala.collection.immutable.Stream;

import java.util.*;

/**
 * Created by abel on 16-10-24.
 */
public class OneConsumerTest {
    public static void main(String[] args) {
        String topic  = "test";
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.0.103:2181");
        props.put("auto.offset.reset","smallest");
        props.put("group.id", "ttest-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig =  new kafka.consumer.ConsumerConfig(props);
        ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        int localConsumerCount = 1;
        topicCountMap.put(topic, localConsumerCount);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        KafkaStream<byte[],byte[]> stream = streams.get(0);
        ConsumerIterator<byte[],byte[]> it = stream.iterator();
        while (it.hasNext())
        {
            System.err.println("-->"+new String(it.next().message()));
        }

        if (consumerConnector != null) consumerConnector.shutdown();
    }
}

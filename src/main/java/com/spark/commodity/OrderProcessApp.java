package com.spark.commodity;

import java.lang.String;
import java.sql.Connection;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.spark.dom.Order;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
import com.alibaba.fastjson.JSON;
import java.sql.DriverManager;
import org.apache.zookeeper.*;


public class OrderProcessApp {
    public static void main(String[] argv) throws Exception {
        // configure Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", IntegerDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Set<String> topics = new HashSet<String>(Arrays.asList("order"));

        // Setup Spark Driver
        SparkConf conf = new SparkConf().setAppName("CommodityApp").setMaster("localhost:9001");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        // Get input stream from Kafka
        JavaInputDStream<ConsumerRecord<Integer, String>> input = KafkaUtils.createDirectStream(
                ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        // TODO: database name?
        Connection channel = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/commodity", "root", "Crash#mysql123");
        // Transform order
        // TODO: process order
        JavaDStream<Tuple2<Integer, Order>> orders = input.map(record ->
                new Tuple2<Integer, Order>(record.key(), JSON.parseObject(record.value(), Order.class)));
        orders.map(order -> {
            // TODO:
            String hostPort = "master:2181,worker1:2181,worker2:2181,worker3:2181";
            ZooKeeper zk = new ZooKeeper(hostPort, 15000, NULL);
            return null;
        });
        orders.print();

        ssc.start();
        ssc.awaitTermination();
    }
}

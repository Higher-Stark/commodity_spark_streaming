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


/**
 * @argv param1 param2 param3
 *  param1:
 */
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

        Set<String> topics = new HashSet<String>(Arrays.asList("test20180430"));

        // Configure mysql and zookeeper
        String zkPorts = "0.0.0.0:2181,192.168.18.144:2181";  // Zookeeper cluster
        // TODO: MySQL database?
        String mysqlJdbc = "jdbc:mysql//0.0.0.0:3386/<database>"; // MysQL config, using SSH channel

        // Setup Spark Driver
        SparkConf conf = new SparkConf().setAppName("CommodityApp").setMaster("localhost:9001");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        jssc.checkpoint("/streaming_checkpoint");

        // Get input stream from Kafka
        JavaInputDStream<ConsumerRecord<String, String>> input =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Transform order
        // TODO: process order
        JavaDStream<Tuple2<String, Order>> orders = input.map(record ->
                new Tuple2<String, Order>(record.key(), JSON.parseObject(record.value(), Order.class)));
        orders.map(order -> {
            // TODO:
            Checker checker = new Checker(zkPorts, mysqlJdbc);
            try{
                checker.startZK();
            } catch (Exception e){
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
            Integer r = checker.check(order._1, order._2);
            if (r < 0){
                System.err.println("Error happens in check for order: " + order._1 + ", error code: "+ r);
            }
            return null;
        });
        // TODO: Remove in the future
        orders.print(); // for DEBUG

        jssc.start();
        jssc.awaitTermination();
    }
}

package com.spark.commodity;

import java.lang.String;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.*;

import com.spark.dom.Item;
import com.spark.dom.Order;
import net.sf.json.JSONObject;
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


/**
 * @argv param1 param2 param3
 *  param1:
 */
public class OrderProcessApp {
    public static void main(String[] argv) throws Exception {
        // configure Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "10.0.0.24:9092,10.0.0.23:9092,10.0.0.48:9092,10.0.0.63:9092"); // build parameter
        kafkaParams.put("bootstrap.servers", "localhost:9092"); // local parameter
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // kafka topics
        Set<String> topics = new HashSet<String>(Arrays.asList("oltest"));

        // Configure mysql and zookeeper
        String zkPorts = "0.0.0.0:2181";  // local parameter
//        String zkPorts = "10.0.0.24:2181,10.0.0.23:2181,10.0.0.48:2181,10.0.0.63:2181"; // build parameter
        // TODO: MySQL database?
        String mysqlJdbc = "jdbc:mysql://10.0.0.63:3306/ds_settlement_system"; // MysQL config, using SSH channel

        // Setup Spark Driver
        SparkConf conf = new SparkConf()
                .setAppName("CommodityApp")
                .setMaster("local[*]"); // local parameter
//                .setMaster("spark://10.0.0.63:7077"); // build parameter
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(3000));

        // Get input stream from Kafka
        JavaInputDStream<ConsumerRecord<String, String>> input =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Transform order
        JavaDStream<String> orders = input.map(
            new Function<ConsumerRecord<String, String>, String>() {
                @Override
                public String call(ConsumerRecord<String, String> record) throws Exception {
                    // init a checker
                    Checker checker = new Checker(zkPorts, mysqlJdbc);
                    try{
                        checker.startZK();
                    } catch (Exception e){
                        System.err.println(e.getMessage());
                        e.printStackTrace();
                        return "failed";
                    }

                    // parse kafka message record
                    String rid = record.key();
                    Order order = new Order();
                    JSONObject order_json = JSONObject.fromObject(record.value());
                    System.out.println(rid + order_json);// log
                    order.user_id = order_json.getString("user_id");
                    order.initiator = order_json.getString("initiator");
                    order.time = order_json.getLong("time");
                    order.items = new ArrayList<Item>();
                    for (int i = 0; i < order_json.getJSONArray("items").size(); i++) {
                        Item it = new Item();
                        it.id = order_json.getJSONArray("items").getJSONObject(i).getString("id");
                        it.number = order_json.getJSONArray("items").getJSONObject(i).getInt("number");
                        order.items.add(it);
                    }
                    System.out.println(rid + '-' + order.toString());

                    // check logic
                    Integer r = checker.check(rid, order);
                    if (r < 0){
                        System.err.println("Error happens in check for order: " + order.toString() + ", error code: "+ r);
                    }

                    return rid + " success";
                }
            }
        );
        // TODO: Remove in the future
        orders.print(); // for DEBUG

        jssc.start();
        jssc.awaitTermination();
    }
}

package com.spark.commodity;

import java.lang.String;
import java.util.*;

import com.spark.dom.Item;
import com.spark.dom.Order;
import net.sf.json.JSONObject;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * @argv param1 param2 param3
 *  param1:
 */
public class OrderProcessApp {
    private static final Logger logger = Logger.getLogger(OrderProcessApp.class);

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
//        String mysqlJdbc = "jdbc:mysql://10.0.0.63:3306/ds_settlement_system"; // build parameter
        String mysqlJdbc = "jdbc:mysql://localhost:3306/ds_settlement_system"; // local parameter

        // DEBUG info -
        logger.info("Zookeeper config: {}" + zkPorts);
        logger.info("MySQL connection config: {}" + mysqlJdbc);

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
        input.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();

            rdd.foreach(record -> {
                Checker checker = new Checker(zkPorts, mysqlJdbc, logger);
                try {
                    checker.startZK();
                }
                catch (Exception e){
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                    return;
                }

                String rid = record.key();
                Order order = new Order();
                JSONObject order_json = JSONObject.fromObject(record.value());
                logger.info(rid + order_json);// log
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

                logger.info(rid + " - " + order.toString());
                String r = checker.check(rid, order);
                if (r.equals(Checker.ERROR_OCCURRED))
                    logger.info("ERROR checking order " + order.toString() + ", error code: " + r);
                r =checker.notifySpring(rid, r);
                if (!r.equals(Checker.SUCCESS_PROCESSED))
                    logger.info("ERROR notifying spring, error code: " + r);
                checker.close();
                logger.info(r + " - " + order.toString());
            });

            ((CanCommitOffsets)input.inputDStream()).commitAsync(offsetRanges);
        });

        jssc.start();
        jssc.awaitTermination();
    }
}

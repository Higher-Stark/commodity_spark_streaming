package com.spark.commodity;

import java.lang.String;
import java.util.*;

import com.spark.dom.Item;
import com.spark.dom.Order;
import com.spark.dom.ZkLock;
import net.sf.json.JSONObject;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.BasicConfigurator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @argv param1 param2 param3
 *  param1:
 */
public class OrderProcessApp {
    private static final Logger logger = LoggerFactory.getLogger(OrderProcessApp.class);

    private static HikariDataSource dataSrc;
    public static void prepareHikari(Properties props) {
        HikariConfig config = new HikariConfig(props);
        dataSrc = new HikariDataSource(config);
    }

    public static void main(String[] argv) throws Exception {
        BasicConfigurator.configure();
        // configure Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.0.0.24:9092,10.0.0.23:9092,10.0.0.48:9092,10.0.0.63:9092"); // build parameter
//        kafkaParams.put("bootstrap.servers", "localhost:9092"); // local parameter
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // kafka topics
        Set<String> topics = new HashSet<String>(Arrays.asList("oltest"));

        // Configure mysql and zookeeper
        // String zkPorts = "0.0.0.0:2181";  // local parameter
        String zkPorts = "10.0.0.24:2181,10.0.0.23:2181,10.0.0.48:2181,10.0.0.63:2181"; // build parameter
        String mysqlJdbc = "jdbc:mysql://10.0.0.63:3306/ds_settlement_system"; // build parameter
        // String mysqlJdbc = "jdbc:mysql://localhost:3306/ds_settlement_system"; // local parameter

        // DEBUG info -
        logger.info("Zookeeper config: {}" + zkPorts);
        logger.info("MySQL connection config: {}" + mysqlJdbc);

        // Setup Hikari Connection Pool
        Properties props = new Properties();
        props.setProperty("jdbcUrl", mysqlJdbc);
        props.setProperty("dataSource.serverName", "10.0.0.63");
        props.setProperty("dataSource.portNumber", "3306");
        props.setProperty("dataSource.user", "root");
        props.setProperty("dataSource.password", "Crash#mysql123");
        props.setProperty("dataSource.cachePrepStmts", "true");
        props.setProperty("dataSource.prepStmtCacheSize", "250");
        props.setProperty("dataSource.prepStmtCacheSqlLimit", "2048");
        props.setProperty("dataSource.useServerPrepStmts", "true");
        props.setProperty("dataSource.useLocalSessionState", "true");
        props.setProperty("dataSource.rewriteBatchedStatements", "true");
        props.setProperty("dataSource.cacheResultSetMetadata", "true");
        props.setProperty("dataSource.cacheServerConfiguration", "true");
        props.setProperty("dataSource.elideSetAutoCommits", "true");
        props.setProperty("dataSource.maintainTimeStats", "false");
        props.put("dataSource.logWriter", logger);
        prepareHikari(props);

        logger.info("Hikari ready");
        // Setup Spark Driver
        SparkConf conf = new SparkConf()
                .setAppName("CommodityApp")
//                .setMaster("local[*]"); // local parameter
                .setMaster("spark://10.0.0.63:7077"); // build parameter
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
                Checker checker = new Checker(zkPorts, logger);
                try {
                    checker.startZK(dataSrc);
                }
                catch (Exception e){
                    logger.error(e.getMessage());
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

                ZkLock lock = new ZkLock(zkPorts, "biglock");
                logger.info(rid + " - " + order.toString());
                lock.lock();
                String r = checker.check(rid, order);
                lock.unlock();
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

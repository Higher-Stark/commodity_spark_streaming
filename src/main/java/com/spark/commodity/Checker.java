package com.spark.commodity;

import com.spark.dom.Item;
import com.spark.dom.Order;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/*
 * Checker implements ZooKeeper Watcher interface.
 * Checker create an connection to Zookeeper and MySQL
 * to process each order
 */
public class Checker implements Watcher{
    public final Integer GOOD_NOT_FOUND = -1;

    private ZooKeeper zk;
    private String hostPort;   // ZooKeeper cluster config

    Connection db_connection; // JDBC connection to MySQL
    private String mysql_hostPort;

    public Checker(String hostPort, String mysql_config) {
        this.hostPort = hostPort;
        this.mysql_hostPort = mysql_config;
    }

    public void startZK() throws IOException, SQLException, ClassNotFoundException {
        zk = new ZooKeeper(hostPort, 15000, this);
        Class.forName("com.mysql.jdbc.Driver");
        db_connection = DriverManager.getConnection(mysql_hostPort, "root", "Crash#mysql123");
        db_connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        db_connection.setAutoCommit(false);  // Turn on Transaction
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public int check(Integer rid, Order order) {
        Collections.sort(order.items);
        // TODO: SQL
        String sql;
        try {
            Statement smt = db_connection.createStatement();
            ResultSet rs;

            // 暂时存储每个商品的余量
            Map<String, Integer> storage = new HashMap<>();
            for (Item i: order.items) {
                sql = "SELECT * FROM commodity WHERE id = " + i.id + " ORDER BY id ASC";
                rs = smt.executeQuery(sql);

                // Good is not found, cancel the order
                if (rs.wasNull()) {
                    System.out.println("Good with id " + i.id + " not found, order cancel!");
                    db_connection.rollback();
                    db_connection.close();
                    return GOOD_NOT_FOUND;
                }

                Integer remain = rs.getInt("inventory");
                if (remain >= i.number) {
                    remain -= i.number;
                    sql = "UPDATE commodity SET inventory = " + remain + " WHERE id = " + i.id;
                    smt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

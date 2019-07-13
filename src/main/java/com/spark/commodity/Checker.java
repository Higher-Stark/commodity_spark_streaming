package com.spark.commodity;

import com.spark.dom.CurrencyRate;
import com.spark.dom.Item;
import com.spark.dom.Order;
import com.zaxxer.hikari.HikariDataSource;
import net.sf.json.JSONObject;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/*
 * Checker implements ZooKeeper Watcher interface.
 * Checker create an connection to Zookeeper and MySQL
 * to process each order
 */
public class Checker implements Watcher{
    public final Integer ORDER_SUCCEED = 0;
    public final Integer ORDER_FAILED = -1;
    public final Integer SQL_EXCEPTION = -2;
    public final Integer CLOSE_ZK_ERROR = -3;
    public final Integer CLOSE_DB_ERROR = -4;

    public final String CHANGE_RATE_JSON = "{\"RMB\":\"2.0\", \"USD\":\"12.0\",\"JPY\":\"0.15\",\"EUR\":\"9.0\"}";

    private static Logger logger;

    private ZooKeeper zk;
    private String hostPort;   // ZooKeeper cluster config

    private Connection db_connection; // JDBC connection to MySQL

    public Checker (String hostPort, Logger logger) {
        this.hostPort = hostPort;
        this.logger = logger;
    }

    public void startZK(HikariDataSource source) throws IOException, SQLException, ClassNotFoundException {
        zk = new ZooKeeper(hostPort, 15000, this);
        db_connection = source.getConnection();
        db_connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        db_connection.setAutoCommit(false);  // Turn on Transaction
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public int notifySpring(String rid, String result_id) {
        //TODO: update ZK node <rid> using value <result_id>
        try {
            zk.setData("/spring/" + rid, result_id.getBytes(), -1);
        }
        catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE)
                logger.error("No node /spring/" + rid + " failed!");
            else
                logger.error("KeeperException " + e.getMessage());
            return -1;
        }
        catch (InterruptedException e) {
            logger.error(e.getMessage());
            return -1;
        }
        catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            return -1;
        }
        return 0;
    }

    private String insertResult(String user_id, String initiator, Integer success, Float totalPaid) throws SQLException{
        // create a result entry & return result_id
        Statement smt = db_connection.createStatement();
        ResultSet rs;
        String new_id;

        String sql = "SELECT MAX(CONVERT(id, SIGNED)) AS id FROM result";
        rs = smt.executeQuery(sql);

        if (rs.wasNull())
            new_id = "1";
        else {
            rs.next();
            new_id = String.valueOf(rs.getInt("id") + 1);
        }
        logger.info("table <result> new id: " + new_id);

        // Change union currency back to initiator currency
        sql = "INSERT INTO result values(" +
                new_id + ", '" + user_id + "', '" + initiator + "', " + success + ", " + totalPaid +
                ")";
        smt.execute(sql);
        return new_id;
    }

    public String check(String rid, Order order) {
        Collections.sort(order.items);
        Float totalPaidUnion = Float.valueOf(0);   //订单总价,以通用货币为单位
        Boolean committed = false;
        String sql, new_id = "";

        // TODO: request on-time exchange rate json instead of CHANGE_RATE_JSON
        byte[] change_rate;
        try {
            change_rate = zk.getData("/settlement/change_rate", false, new Stat());
        }
        catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                logger.info("zNode /settlement/change_rate not exists!");
            }
            else
                logger.info("KeeperException !" + e.getMessage());
            return "-1";
        }
        catch (InterruptedException e) {
            logger.info("Server transaction interrupted!" + e.getMessage());
            return "-1";
        }

        CurrencyRate currencyRate = new CurrencyRate(JSONObject.fromObject(new String(change_rate)));
//        CurrencyRate currencyRate = new CurrencyRate(JSONObject.fromObject(CHANGE_RATE_JSON));

        boolean rs_inserted = false;
        try {
            Statement smt = db_connection.createStatement();
            ResultSet rs;

            // 暂时存储每个商品的余量
            Map<String, Integer> storage = new HashMap<>();
            for (Item i : order.items) {
                sql = "SELECT * FROM commodity WHERE id = " + i.id + " ORDER BY id ASC";
                rs = smt.executeQuery(sql);

                // Good is not found, cancel the order
                if (rs.wasNull()) {
                    logger.info("Good with id " + i.id + " not found, order cancel!");
                    db_connection.rollback(); // withdraw previous modification
                    new_id = insertResult(order.user_id, order.initiator, 0, Float.valueOf(0));
                    rs_inserted = true;
                    db_connection.commit();
                    committed = true;
                    return new_id;
                }

                rs.next();
                Integer remain = rs.getInt("inventory");
                Float price = rs.getFloat("price");
                String currency = rs.getString("currency");
                // Good is not enough, cancel the order
                if (remain < i.number) {
                    logger.info("Good with id " + i.id + " not enough, required " + i.number + ", left " + remain + ". Order cancel!");
                    db_connection.rollback(); // withdraw previous modification
                    new_id = insertResult(order.user_id, order.initiator, 0, Float.valueOf(0));
                    rs_inserted = true;
                    db_connection.commit();
                    committed = true;
                    return new_id;
                } else {
                    remain -= i.number;
                    sql = "UPDATE commodity SET inventory = " + remain + " WHERE id = " + i.id;
                    smt.executeUpdate(sql);
                    totalPaidUnion += i.number * price * currencyRate.getRate(currency);
                }
            }
            // Successfully processed all the goods in the order, commit, note that totalPaidUnion is changed to totalPaid
            new_id = insertResult(order.user_id, order.initiator, 1, totalPaidUnion/currencyRate.getRate(order.initiator));
            rs_inserted = true;
            db_connection.commit();
            committed = true;
            return new_id;
        }
        catch (SQLException e) {// exception occurs, cancel the order
            logger.error(e.getMessage());
            e.printStackTrace();
            try{
                if (rs_inserted)
                db_connection.rollback();
            }
            catch (SQLException e1) {
                logger.error(e1.getMessage());
                e1.printStackTrace();
            }
            finally {
                return "-1";
            }
        }
    }

    public int close() {
        int ret = 0;
        try {
            zk.close();
        }
        catch (InterruptedException e) {
            logger.error(e.getMessage());
            ret = CLOSE_ZK_ERROR;
        }
        try {
            db_connection.close();
        }
        catch (SQLException ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
            ret = CLOSE_DB_ERROR;
        }
        return ret;
    }
}

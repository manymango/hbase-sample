package com.manymango;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author manymango
 * @time 2019/12/11 19:37
 * @description
 */
public class HbaseInstance {

    private static Connection connection;

    public static Connection getConnection() throws IOException {
        if (null == connection) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }



    public static Table getTable(String name) throws IOException {
        Connection connection = getConnection();
        return connection.getTable(TableName.valueOf(name));
    }



}

package com.manymango;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author manymango
 * @time 2019/12/11 19:37
 * @description
 */
public class HbaseInstance {

    private static Connection connection;

    public static Connection getConnection() {
        if (null == connection) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }



}

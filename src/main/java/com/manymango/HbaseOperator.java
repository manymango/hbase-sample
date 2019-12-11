package com.manymango;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author manymango
 * @time 2019/12/11 19:51
 * @description
 */
public class HbaseOperator {

    public static boolean createTable() {
        Connection connection = HbaseInstance.getConnection();
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("test");
            Collection<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
            ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes())
                    .setMaxVersions(5)
                    .build();
            families.add(columnFamily);
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamilies(families)
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static void main(String[] args) {
        Connection connection = HbaseInstance.getConnection();
        System.out.println("xixi");
    }

}

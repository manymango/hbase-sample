package com.manymango;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author manymango
 * @time 2019/12/11 19:51
 * @description
 */
public class HbaseOperator {

    public static boolean createTable(String name) throws IOException {
        Connection connection = HbaseInstance.getConnection();
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
        admin.createTable(tableDescriptor);
        return true;
    }


    public static List<TableDescriptor> listTableDescriptor() throws IOException {
        Connection connection = HbaseInstance.getConnection();
        Admin admin = connection.getAdmin();
        return admin.listTableDescriptors();
    }


    public static TableDescriptor getTableDescriptor(String name) throws IOException {
        Table table = HbaseInstance.getTable(name);
        return table.getDescriptor();
    }


    public static Result get(String tableName, String rowKey, String cf) throws IOException {
        Table table = HbaseInstance.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        if (StringUtils.isNotEmpty(cf)) {
            get.addFamily(cf.getBytes());
        }
        return table.get(get);
    }


    public static void put(String tableName, String rowKey, String cf, String qualifier, String value) throws IOException {
        Table table = HbaseInstance.getTable(tableName);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(cf.getBytes(), qualifier.getBytes(), value.getBytes());
        table.put(put);
    }


    public static List<Result> scan(String tableName, String startKey, String stopKey) throws IOException {
        Table table = HbaseInstance.getTable(tableName);
        Scan scan = new Scan();
        scan.setLimit(1000);
        if (StringUtils.isNotEmpty(startKey)) {
            scan.withStartRow(startKey.getBytes(), false);
        }

        if (StringUtils.isNotEmpty(stopKey)) {
            scan.withStopRow(stopKey.getBytes(), false);
        }

        ResultScanner resultScanner = table.getScanner(scan);
        List<Result> results = new ArrayList<Result>();
        Result result = resultScanner.next();
        while (null != result) {
            results.add(result);
            result = resultScanner.next();
        }
        return results;
    }





    public static void main(String[] args) throws IOException, InterruptedException {
        Table table = HbaseInstance.getTable("test");
        System.out.println("xixi");
        while (true) {
            Thread.sleep(500);
        }
    }

}

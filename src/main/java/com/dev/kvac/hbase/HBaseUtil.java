package com.dev.kvac.hbase;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.HMaster;

public final class HBaseUtil {

    public static void addTable(String keyspace, List<String> columnFamilies)
        throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor();
        desc.setName(keyspace.getBytes());

        for (int i = 0; i < columnFamilies.size(); i++) {
            String family = columnFamilies.get(i);
            HColumnDescriptor colFamily = new HColumnDescriptor(family);
            desc.addFamily(colFamily);
        }
        admin.createTable(desc);
    }

    public static void addColumnFamily(String keyspace, String columnFamily)
        throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HColumnDescriptor column = new HColumnDescriptor(columnFamily);
        // admin.deleteColumn(keyspace, columnFamily);
        admin.addColumn(keyspace, column);

    }

    public static void disableTable(String keyspace) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(keyspace);
    }

    public static void deleteTable(String keyspace) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.deleteTable(keyspace);
    }

    public static String get(String keyspace, String rowKey, String column,
        long timestamp) throws Exception {
        String columnValue = null;

        HTable htable = new HTable(keyspace);
        Get get = new Get(rowKey.getBytes());
        get = get.setTimeStamp(timestamp);
        get = get.setMaxVersions();

        Result res = htable.get(get);

        KeyValue[] data = res.raw();

        for (int i = 0; i < data.length; i++) {
            KeyValue d = data[i];
            String family = new String(data[i].getFamily());
            String qualifier = new String(data[i].getQualifier());
            if (qualifier.toLowerCase().equals(column.toLowerCase())) {
                columnValue = new String(d.getValue());
                System.out.println(data[i].toString() + " Family:" + family
                    + " Qualifier:" + qualifier + " Value:" + columnValue);
                break;
            }
        }

        return columnValue;
    }

    public static void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value, long timestamp) throws Exception {
        HTable htable = new HTable(keyspace);
        Put put = new Put(rowKey.getBytes());

        put = put.add(columnFamily.getBytes(), columnKey.getBytes(), timestamp,
            value.getBytes());
        htable.put(put);

    }

    public static void clean(String keyspace, String rowKey) throws Exception {
        HTable htable = new HTable(keyspace);
        Delete rowToDelete = new Delete(rowKey.getBytes());
        htable.delete(rowToDelete);
    }
}
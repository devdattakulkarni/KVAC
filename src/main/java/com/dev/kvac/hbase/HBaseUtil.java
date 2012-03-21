package com.dev.kvac.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public final class HBaseUtil {

    public static String get(String keyspace, String rowKey, String column)
        throws Exception {
        String columnValue = null;

        HTable htable = new HTable(keyspace);
        Get get = new Get(rowKey.getBytes());
        Result res = htable.get(get);
        // System.out.println(res.getBytes().toString());
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

}
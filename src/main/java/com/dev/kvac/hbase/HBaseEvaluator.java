package com.dev.kvac.hbase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVStoreInterface;

public class HBaseEvaluator extends Evaluator {

    private static Logger logger = LoggerFactory
        .getLogger(HBaseEvaluator.class);

    public HBaseEvaluator(KVStoreInterface kvstore, String storeType) {
        super(kvstore, storeType);
    }

    public String getAttributeValue(String key, String expr) {

        String columnValue = null;

        String keyspace = parseKeySpace(expr);
        String columnFamily = parseColumnFamily(expr);

        if (columnFamily == null) {
            return columnValue;
        }

        String[] columnNameValue = parseColumn(expr);
        String column = columnNameValue[0];
        String parameterizedColValue = columnNameValue[1];
        String rowKey = parseRowKey(key, expr);

        try {
            if (storeType.equalsIgnoreCase(HBASE)) {
                columnValue = HBaseUtil.get(keyspace, rowKey, column, 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return columnValue;
    }

}

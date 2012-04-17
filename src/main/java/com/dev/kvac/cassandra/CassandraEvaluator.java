package com.dev.kvac.cassandra;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVStoreInterface;

public class CassandraEvaluator extends Evaluator {

    private static Logger logger = LoggerFactory
        .getLogger(CassandraEvaluator.class);

    public CassandraEvaluator(KVStoreInterface kvstore, String storeType) {
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

            byte[] val = (byte[]) ((CassandraAccessor) kvstore)
                .getCassandraUtil().get(columnFamily, rowKey, column);

            BufferedInputStream buffer = new BufferedInputStream(
                new ByteArrayInputStream(val));
            ObjectInput input = new ObjectInputStream(buffer);

            columnValue = (String) input.readObject();

            if (parameterizedColValue != null) {
                String actualColValue = kvstore
                    .getRuntimeParameterValues(parameterizedColValue);
                if (columnValue.contains(actualColValue)) {
                    return actualColValue;
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return columnValue;
    }
}

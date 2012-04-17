package com.dev.kvac.mongodb;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVStoreInterface;

public class MongoDBEvaluator extends Evaluator {

    public MongoDBEvaluator(KVStoreInterface kvstore, String storeType) {
        super(kvstore, storeType);
        // TODO Auto-generated constructor stub
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
        columnValue = ((MongoDBAccessor) kvstore).getUtil().get(keyspace,
            columnFamily, rowKey, column);

        return columnValue;
        
    }

}

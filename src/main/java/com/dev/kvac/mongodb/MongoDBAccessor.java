package com.dev.kvac.mongodb;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVStoreInterface;

public class MongoDBAccessor implements KVStoreInterface {

    String user;
    Evaluator evaluator;
    MongoDBUtil mongoUtil;

    public MongoDBAccessor(String host, int port, String database, String user) throws Exception {
        this.user = user;
        this.evaluator = new Evaluator(this, "mongodb");        
        this.mongoUtil = new MongoDBUtil(host, port);
    }

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey) throws Exception {
        String value = null;
        
        return value;
    }

    public String getUser() {
        return user;
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value) throws Exception {

    }

}

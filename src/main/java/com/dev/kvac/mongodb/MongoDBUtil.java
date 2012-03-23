package com.dev.kvac.mongodb;

import java.util.Iterator;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDBUtil {

    Mongo m = null;
    DB db = null;

    public MongoDBUtil(String host, int port) throws Exception {
        this.m = new Mongo(host, port);
    }

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey) {
        String value = null;
        this.db = m.getDB(keyspace);
        DBCollection coll = db.getCollection(columnFamily);
        DBCursor cursor = coll.find();

        Iterator<DBObject> docIter = cursor.iterator();
        while (docIter.hasNext()) {
            DBObject myDoc = docIter.next();

            // Assumes that each document has a field with the name = rowKey
            if (myDoc.containsField(rowKey)) {
                System.out.println(myDoc);
                value = (String) myDoc.get(columnKey);
            }
        }
        return value;
    }

}

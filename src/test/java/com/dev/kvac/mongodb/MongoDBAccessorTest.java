package com.dev.kvac.mongodb;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class MongoDBAccessorTest {
    MongoDBAccessor mongodb;

    @Before
    public void setup() {
    }
    
    @Test
    public void testGetSuccess() throws Exception {
        System.out.println("MongoDB Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        String host = "localhost";
        int port = 27017;
 
        mongodb = new MongoDBAccessor(host, port, user, policyFilePath);
        
        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";
        mongodb.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        mongodb.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        String colValue = mongodb.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
        Assert.assertEquals(columnValue, colValue);
    }
    
    @Test
    public void testGetFailure() throws Exception {
        System.out.println("MongoDB Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        String host = "localhost";
        int port = 27017;
 
        mongodb = new MongoDBAccessor(host, port, user, policyFilePath);
        
        String keyspace = "PatientInfoSystem";
        
        mongodb.clean(keyspace);
        
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "john";
        mongodb.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        columnValue = "jack";
        mongodb.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        String colValue = mongodb.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
        Assert.assertNull(colValue);
    }
}

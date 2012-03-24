package com.dev.kvac.hbase;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class HBaseAccessorTest {
    
    HBaseAccessor hbase;
    
    @Before
    public void setup() {        
    }

    @Test
    public void testGetSuccess() throws Exception {
        System.out.println("HBase Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);
        
        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";
        
        hbase.clean(keyspace, rowKey);
        
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        
        hbase.clean(keyspace, rowKey);
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        String colValue = hbase.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
        Assert.assertEquals(columnValue, colValue);
    }
    
    @Test
    public void testGetFailure() throws Exception {
        System.out.println("HBase Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);
        
        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";
        
        hbase.clean(keyspace, rowKey);
        
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "john";
        columnKey = "name";
        columnValue = "john";
        
        hbase.clean(keyspace, rowKey);
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        String colValue = hbase.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
        Assert.assertNull(colValue);
    }    
}

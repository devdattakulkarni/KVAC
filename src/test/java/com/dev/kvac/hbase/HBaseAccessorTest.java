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
    public void testGet() throws Exception {
        System.out.println("HBase Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);
        
        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        
        String colValue = hbase.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
        Assert.assertEquals(columnValue, colValue);
    }
}

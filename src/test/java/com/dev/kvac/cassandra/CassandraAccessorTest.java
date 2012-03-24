package com.dev.kvac.cassandra;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class CassandraAccessorTest {
    CassandraAccessor cassandra;

    @Before
    public void setup() {
    }

    @Test
    public void testGetSuccess() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/Policy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";

        accessor.dropColumnFamily(columnFamily);
        accessor.addColumnFamily(keyspace, columnFamily);
        //accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        
        accessor.dropColumnFamily(columnFamily);
        accessor.addColumnFamily(keyspace, columnFamily);
        //accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue);

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey);
        System.out.println("Column Value:" + colValue);
        
        Assert.assertEquals(columnValue, colValue);
    }
    
    @Test
    public void testGetFailure() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/Policy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "john";

        accessor.dropColumnFamily(columnFamily);
        accessor.addColumnFamily(keyspace, columnFamily);
        //accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue);
        
        columnFamily = "Patient";
        rowKey = "john";
        columnKey = "name";
        columnValue = "jack";
        
        accessor.dropColumnFamily(columnFamily);
        accessor.addColumnFamily(keyspace, columnFamily);
        //accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue);

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey);
        System.out.println("Column Value:" + colValue);
        
        Assert.assertNull(colValue);
    }
}

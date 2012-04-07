package com.dev.kvac.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocialShareTest {
    CassandraAccessor cassandra;
    private final Logger log = LoggerFactory.getLogger(CassandraAccessor.class);

    @Before
    public void setup() {
    }

    @Test
    public void testPolicy1_ARowAccessPolicy() throws Exception {
        String keyspace = "SocialShare";
        CassandraAccessor accessor = getCassandraAccessor(keyspace);
        
        String columnFamily = "Person";
        String rowKey = "jr";
        String columnKey = "name";
        String columnValue = "jr";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "family";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "plans";
        String plans = "Visit China";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, plans, 1);
        
        rowKey = "devdatta";
        columnKey = "name";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        String queryColumnFamily = "Person";
        String queryRowKey = "jr";
        String queryColumnKey = "name";

        String colValue = accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1);
        System.out.println("Column Value:{" + colValue + "}");
        List<String> expectedList = new ArrayList<String>();
        expectedList.add("name:jr");
        expectedList.add("family:devdatta");
        expectedList.add("plans:Visit China");
        StringTokenizer st = new StringTokenizer(colValue, "|");
        while(st.hasMoreTokens()) {
            String token = st.nextToken();
            if (expectedList.contains(token)) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
        }
        
        //Assert.assertEquals(plans, colValue);
        
        rowKey = "purandare";
        columnKey = "family";
        columnValue = "aai purandare";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        queryRowKey = "purandare";
        queryColumnKey = "plans";
        colValue = accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertNull(plans,colValue);
        
    }    
    
    @Test
    public void testPolicy2_AColumnAccessPolicy() throws Exception {
        String keyspace = "SocialShare";
        CassandraAccessor accessor = getCassandraAccessor(keyspace);
        
        String columnFamily = "Person";
        String rowKey = "jr";
        String columnKey = "name";
        String columnValue = "jr";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "family";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "plans";
        String plans = "Visit China";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, plans, 1);
        
        rowKey = "devdatta";
        columnKey = "name";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        String queryColumnFamily = "Person";
        String queryRowKey = "jr";
        String queryColumnKey = "plans";

        String colValue = accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertEquals(plans, colValue);
        
        rowKey = "purandare";
        columnKey = "family";
        columnValue = "aai purandare";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        queryRowKey = "purandare";
        queryColumnKey = "plans";
        colValue = accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertNull(plans,colValue);
        
    }
    
    
    
    private CassandraAccessor getCassandraAccessor(String keyspace)
        throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/SocialShareAppPolicy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        return accessor;
    }

}

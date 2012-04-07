package com.dev.kvac.cassandra;

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
    public void testPolicy1Success() throws Exception {
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

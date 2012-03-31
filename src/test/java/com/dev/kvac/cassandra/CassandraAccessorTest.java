package com.dev.kvac.cassandra;

import java.util.Map;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class CassandraAccessorTest {
    CassandraAccessor cassandra;
    private final Logger log = LoggerFactory.getLogger(CassandraAccessor.class);

    @Before
    public void setup() {
    }

    @Test
    public void testCreationOfPermissionCF() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/Policy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamily = "Permission";
        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);

        Map<String, Node> resPolicyMap = accessor.getResourcePolicyMap();

        for (String key : resPolicyMap.keySet()) {
            Node conditionNode = resPolicyMap.get(key);
            String columnKey = "permission";
            String columnValue = conditionNode.toString();
            accessor
                .put(keyspace, columnFamily, key, columnKey, columnValue, 1);
        }

        for (String key : resPolicyMap.keySet()) {
            String column = "permission";
            String value = accessor.getCassandraUtil().get(columnFamily, key,
                column);
            System.out.println(value);
        }
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

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        // accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        // accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey, 1);
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

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        // accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnFamily = "Patient";
        rowKey = "john";
        columnKey = "name";
        columnValue = "jack";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        // accessor.delete(columnFamily, rowKey, columnKey);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey, 1);
        System.out.println("Column Value:" + colValue);

        Assert.assertNull(colValue);
    }
}

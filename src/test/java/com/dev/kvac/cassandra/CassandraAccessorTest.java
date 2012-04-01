package com.dev.kvac.cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
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
        columnValue = "real name is jack";

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

    @Test
    public void stressTestCassandraWriteRead() throws Exception {
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

        Long startTime = System.currentTimeMillis();
        Long timestamp_put = new Long(1);
        long numOfIterations = 100000;
        for (int i = 0; i < numOfIterations; i++) {
            accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue,timestamp_put + i);
        }

        Long timestamp_get = new Long(1);
        for (int i = 0; i < numOfIterations; i++) {
            accessor.direct_get(keyspace, columnFamily, rowKey, columnKey,
                timestamp_get + i);
        }

        Long endTime = System.currentTimeMillis();
        Long totalTime = endTime - startTime;
        System.out.println("Total Time:" + totalTime);
    }

    @Test
    public void stressTestJavaFileSystemWriteRead() throws Exception {
        try {
            // Create file
            FileWriter fstream = new FileWriter("out.txt");
            BufferedWriter out = new BufferedWriter(fstream);

            Long startTime = System.currentTimeMillis();
            Long timestamp_put = new Long(1);
            long numOfIterations = 100000;
            for (int i = 0; i < numOfIterations; i++) {
                out.write("Val " + i + "\n");
            }
            // Close the output stream
            out.close();

            FileInputStream finputstream = new FileInputStream("out.txt");
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(finputstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            // Read File Line By Line
            while ((strLine = br.readLine()) != null) {
            }
            // Close the input stream
            in.close();
            
            Long endTime = System.currentTimeMillis();
            
            Long totalTime = endTime - startTime;
            
            System.out.println("Total Time:" + totalTime);

        } catch (Exception e) {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }
}

package com.dev.kvac.cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.Map;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
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

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
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
    public void testGetAllColumnsForARow() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
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
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "strengths";
        columnValue = "learner, responsibility, achiever, input";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);
        
        columnKey = "education";
        columnValue = "PhD";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String colValue = accessor.getCassandraUtil().getRow(columnFamily, rowKey);
        System.out.println("Column Value:" + colValue);
    }
    
    @Test
    public void testGetSuccessForARowKey() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
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
    public void testGetFailureForARowKey() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
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

        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey, 1);
        System.out.println("Column Value:" + colValue);

        Assert.assertNull(colValue);
    }

    @Test
    public void testGetSuccessForCurrentTimeWithinTimeRange() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamilyPatient = "Patient";
        String rowKeyPatient = "jack";
        String columnKeyPatient = "medical_history";
        String columnValuePatient = "Cough";

        try {
            accessor.dropColumnFamily(columnFamilyPatient);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamilyPatient);

        accessor.put(keyspace, columnFamilyPatient, rowKeyPatient,
            columnKeyPatient, columnValuePatient, 1);

        String columnFamilyNurse = "Nurse";
        String rowKeyNurse = "devdatta";
        String columnKeyNurse = "work_hours";

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
        DateTime startDate = date.minusDays(2);
        DateTime endDate = date.plusDays(1);

        String startWorkHour = dateFormat.format(startDate.toDate());
        String endWorkHour = dateFormat.format(endDate.toDate());
        // String columnValueNurse = "2012/01/01 12:00:00-2012/04/30 12:00:00";
        String columnValueNurse = startWorkHour + "-" + endWorkHour;

        try {
            accessor.dropColumnFamily(columnFamilyNurse);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamilyNurse);

        accessor.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);

        String colValue = accessor.get(keyspace, columnFamilyPatient,
            rowKeyPatient, columnKeyPatient, 1);
        System.out.println("Column Value:" + colValue);

        Assert.assertEquals(columnValuePatient, colValue);
    }

    @Test
    public void testGetFailureForCurrentTimeNotWithinTimeRange()
        throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamilyPatient = "Patient";
        String rowKeyPatient = "jack";
        String columnKeyPatient = "medical_history";
        String columnValuePatient = "Cough";

        try {
            accessor.dropColumnFamily(columnFamilyPatient);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamilyPatient);

        accessor.put(keyspace, columnFamilyPatient, rowKeyPatient,
            columnKeyPatient, columnValuePatient, 1);

        String columnFamilyNurse = "Nurse";
        String rowKeyNurse = "devdatta";
        String columnKeyNurse = "work_hours";

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
        DateTime startDate = date.minusDays(2);
        DateTime endDate = date.minusDays(1);

        String startWorkHour = dateFormat.format(startDate.toDate());
        String endWorkHour = dateFormat.format(endDate.toDate());
        // String columnValueNurse = "2012/01/01 12:00:00-2012/03/30 12:00:00";
        String columnValueNurse = startWorkHour + "-" + endWorkHour;

        try {
            accessor.dropColumnFamily(columnFamilyNurse);
        } catch (InvalidRequestException invalidRequest) {
            log.info(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamilyNurse);

        accessor.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);

        String colValue = accessor.get(keyspace, columnFamilyPatient,
            rowKeyPatient, columnKeyPatient, 1);
        System.out.println("Column Value:" + colValue);

        Assert.assertNull(colValue);
    }

    @Ignore
    @Test
    public void stressTestCassandraWriteRead() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
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
            accessor.put(keyspace, columnFamily, rowKey, columnKey,
                columnValue, timestamp_put + i);
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

    @Ignore
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

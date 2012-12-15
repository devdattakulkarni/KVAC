package com.dev.kvac.cassandra;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSizeTest {
    
    private CassandraAccessor accessor;
    private final Logger log = LoggerFactory.getLogger(DataSizeTest.class);
    String user = "devdatta";
    String password = "devdatta";
    String keyspace = "PatientInfoSystem";
    String server = "localhost";
    int port = 9170;

    String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
         
    @Before
    public void setup() throws Exception {
        accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);
    }
    
    @Test
    public void testDirectGet() throws Exception {
        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "Date_Of_Operation";
        long timestamp = 0;
        String value = "December 3, 2012";
        
        accessor.put(keyspace, columnFamily, rowKey, columnKey, value, timestamp);
        
        String dateOfRelease = accessor.direct_get(keyspace, columnFamily, rowKey, columnKey, timestamp);
        log.debug("Date of Admission:" + dateOfRelease);
    }
    
    @Test
    public void insert1MBKeys() throws Exception {
        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "Date_Of_Checkup";
        long timestamp = 0;
        String value = "December 3, 2012";
        
        long start = System.currentTimeMillis();
        int dataSize = 1;
        for(int i=0; i<dataSize; i++) {
            accessor.put(keyspace, columnFamily, rowKey + i, columnKey, value + " " + i, timestamp);
        }
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        double avgInsertTimePerRecord = totalTime / dataSize;
        log.debug("Total insertion time:" + totalTime);
        log.debug("Per record insertion time:" + avgInsertTimePerRecord);
    }
    
    @Test
    public void read1MBKeys() throws Exception {
        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "Date_Of_Checkup";
        long timestamp = 0;
        String value = "December 3, 2012";
        
        long start = System.currentTimeMillis();
        int dataSize = 1;
        //for(int i=0; i<dataSize; i++) {
            String dateOfCheckup = accessor.direct_get(keyspace, columnFamily, rowKey + 0, columnKey, timestamp);
            //log.debug("Date of Checkup:" + dateOfCheckup);
        //}
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        double avgInsertTimePerRecord = totalTime / dataSize;
        log.debug("Total read time:" + totalTime);
        log.debug("Per record insertion time:" + avgInsertTimePerRecord);
    }    
    
    @Test
    public void delete256MBKeys() throws Exception {
        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "Date_Of_Checkup";
        long timestamp = 0;
        String value = "December 3, 2012";
        
        long start = System.currentTimeMillis();
        int dataSize = 268435456;
        for(int i=0; i<dataSize; i++) {
            accessor.delete(columnFamily, rowKey, columnKey);
        }
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        log.debug("Total insertion time:" + totalTime);
        log.debug("Per record insertion time:" + totalTime / dataSize);
    }
}
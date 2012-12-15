package com.dev.kvac.cassandra;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientInfoSystemCODASPYExperimentsKVACINCassandra {

    private CassandraAccessor accessor;
    private final Logger log = LoggerFactory.getLogger(DataSizeTest.class);
    String user = "devdatta";
    String password = "devdatta";
    String keyspace = "PatientInfoSystem";
    String server = "localhost";
    int port = 9170;
    private int numberOfExperiments = 1;

    String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";

    @Before
    public void setup() throws Exception {
        accessor = new CassandraAccessor(policyFilePath, user, password,
            keyspace, server, port);
    }
    
    @Test
    public void insert10kKeys() throws Exception {
        String columnFamily = "Patient";
        String rowKey = "jack";
        String columnKey = "location";
        long timestamp = 0;
        String value = "ward";
        
        long start = System.currentTimeMillis();
        int dataSize = 10240;
        for(int i=0; i<dataSize; i++) {
            accessor.put(keyspace, columnFamily, rowKey + i, columnKey, value + " " + i, timestamp);
        }
        
         columnFamily = "Nurse";
         rowKey = "dev";
         columnKey = "location";
         timestamp = 0;
         value = "ward";
        
        for(int i=0; i<dataSize; i++) {
            accessor.put(keyspace, columnFamily, rowKey + i, columnKey, value + " " + i, timestamp);
        }
        
        columnFamily = "Doctor";
        rowKey = "doc";
        columnKey = "location";
        timestamp = 0;
        value = "ward";
       
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
    public void testGetColumnWithOneQueryToCFKVAC_IN_Cassandra()
        throws Exception {

        String columnFamilyPatient = "Patient";
        String rowKeyPatient = "jack";
        String columnKeyPatient = "medical_history";
        String columnValuePatient = "Cough";

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
        String columnValueNurse = startWorkHour + "-" + endWorkHour;

        accessor.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);

        // doGet(accessor, keyspace, columnFamilyPatient, rowKeyPatient,
        // columnKeyPatient);
        long start = System.currentTimeMillis();
        String patient_medications = accessor.direct_get(keyspace,
            columnFamilyPatient, rowKeyPatient, columnKeyPatient, 1);
        long end = System.currentTimeMillis();
        log.info("Patient medications:" + patient_medications);
        long totalTime = end - start;
        log.info("Total Time:" + totalTime);
    }

    @Test
    public void testGetColumnWithOneQueryToCFKVAC_IN_Cassandra_10KRows()
        throws Exception {

        String columnFamilyPatient = "Patient";
        String rowKeyPatient = "jack";
        String columnKeyPatient = "medical_history";
        String columnValuePatient = "Cough";

        int data = 10240;

        for (int i = 0; i < data; i++) {
            accessor.put(keyspace, columnFamilyPatient, rowKeyPatient + i,
                columnKeyPatient, columnValuePatient, 1);
        }

        String columnFamilyNurse = "Nurse";
        String rowKeyNurse = "devdatta";
        String columnKeyNurse = "work_hours";

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
        DateTime startDate = date.minusDays(2);
        DateTime endDate = date.plusDays(1);

        String startWorkHour = dateFormat.format(startDate.toDate());
        String endWorkHour = dateFormat.format(endDate.toDate());
        String columnValueNurse = startWorkHour + "-" + endWorkHour;

        accessor.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);

        // doGet(accessor, keyspace, columnFamilyPatient, rowKeyPatient,
        // columnKeyPatient);
        long start = System.currentTimeMillis();
        for (int i = 0; i < data; i++) {
            String patient_medications = accessor.direct_get(keyspace,
                columnFamilyPatient, rowKeyPatient + i, columnKeyPatient, 1);
            // log.info("Patient medications:" + patient_medications);
        }
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        log.info("Total Time:" + totalTime);
    }

    @Test
    public void testGetColumnWithTwoQueriesToCFInKVACPolicy() throws Exception {

        String columnFamily = "Nurse";
        String rowKey = "devdatta";
        String columnKey = "location";
        String columnValue = "ward-2";

        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnFamily = "Patient";
        rowKey = "james";
        columnKey = "location";
        columnValue = "ward-2";

        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnKey = "patient_info";
        columnValue = "12345 Hogwarts Drive, Pippin Street, JacksonHole P";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        // doGet(accessor, keyspace, columnFamily, rowKey, columnKey);
        do_direct_get(accessor, keyspace, columnFamily, rowKey, columnKey);
    }
    
    @Test
    public void testGetColumnWithFiveQueriesToCFInKVACPolicy() throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-3";

        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-3";

        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "yk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "patient_reports_40";
        patientColVal = "Has cold. No flu symptoms - 40 12345 40";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "yk";
        String doctorColkey = "location";
        String doctorColVal = "ward-3";

        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports_40";
        
        do_direct_get(accessor, keyspace, queryColumnFamily, queryRowKey, queryColumnKey);
    }
    
    @Test
    public void testGetColumnWithTenQueriesToCFInKVACPolicy()
        throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";
        
        //accessor.delete(nurseColFamily, nurseRowkey, nurseColkey);

        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "jedi";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "nk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);
        
        patientColkey = "patient_reports_10";
        patientColVal = "Has cold. No flu symptoms - 5 12345 Jacksonhole";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "nk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "jedi";
        String queryColumnKey = "patient_reports_10";
        
        do_direct_get(accessor, keyspace, queryColumnFamily, queryRowKey, queryColumnKey);
    }    

    private void do_direct_get(CassandraAccessor accessor, String keyspace,
        String queryColumnFamily, String queryRowKey, String queryColumnKey) throws Exception {
        long start = System.currentTimeMillis();
        String patient_medications = accessor.direct_get(keyspace,
            queryColumnFamily, queryRowKey, queryColumnKey, 1);
        long end = System.currentTimeMillis();
        log.info("Patient medications:" + patient_medications);
        long totalTime = end - start;
        log.info("Total Time:" + totalTime);
    }

    private void doGet(CassandraAccessor accessor, String keyspace,
        String queryColumnFamily, String queryRowKey, String queryColumnKey)
        throws Exception {
        double totalTime = 0;
        long[] times = new long[numberOfExperiments];
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();

            String colValue = (String) accessor.get(keyspace,
                queryColumnFamily, queryRowKey, queryColumnKey, 1, null);

            long end = System.currentTimeMillis();
            totalTime += (end - start);
            times[i] = (end - start);
            log.info("Time: " + (end - start));
            log.info("Column Value:" + colValue);
        }
        log.info("Total time:" + totalTime);
        Double avgTime = totalTime / numberOfExperiments;
        DecimalFormat df = new DecimalFormat("#.##");
        log.info("Avg time:" + df.format(avgTime));
        Arrays.sort(times);
        long median = times[numberOfExperiments / 2];
        log.info("Median time:" + median);
    }
}
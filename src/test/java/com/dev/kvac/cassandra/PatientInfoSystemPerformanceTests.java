package com.dev.kvac.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientInfoSystemPerformanceTests {
    CassandraAccessor cassandra;
    private final Logger log = LoggerFactory.getLogger(CassandraAccessor.class);
    private int numberOfExperiments = 10;

    @Before
    public void setup() {
    }

    @Test
    public void testGetRowWithNoKVACPolicy() throws Exception {
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
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnKey = "strengths";
        columnValue = "intellection, responsibility, learner, achiever, input";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnKey = "education";
        columnValue = "PhD";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = accessor.getCassandraUtil().getRow(columnFamily,
                rowKey);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }

    @Test
    public void testGetColumnWithOneQueryToCFInKVACPolicy() throws Exception {
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
            log.debug(invalidRequest.getMessage());
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
        String columnValueNurse = startWorkHour + "-" + endWorkHour;

        try {
            accessor.dropColumnFamily(columnFamilyNurse);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamilyNurse);

        accessor.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace,
                columnFamilyPatient, rowKeyPatient, columnKeyPatient, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }

    @Test
    public void testGetColumnWithTwoQueriesToCFInKVACPolicy() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamily = "Nurse";
        String rowKey = "devdatta";
        String columnKey = "location";
        String columnValue = "ward-2";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "location";
        columnValue = "ward-2";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnKey = "patient_info";
        columnValue = "12345 Hogwarts Drive, Pippin Street, JacksonHole";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace, columnFamily,
                rowKey, columnKey, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }

    @Test
    public void testGetColumnWithFourQueriesToCFInKVACPolicy() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        try {
            accessor.dropColumnFamily(nurseColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, nurseColFamily);
        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        try {
            accessor.dropColumnFamily(patientColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, patientColFamily);
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "pk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "patient_reports";
        patientColVal = "Has cold. No flu symptoms";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "pk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        try {
            accessor.dropColumnFamily(doctorColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, doctorColFamily);
        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports";

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace,
                queryColumnFamily, queryRowKey, queryColumnKey, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }
    
    @Test
    public void testGetColumnWithEightQueriesToCFInKVACPolicy() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicyForTest.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        try {
            accessor.dropColumnFamily(nurseColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, nurseColFamily);
        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        try {
            accessor.dropColumnFamily(patientColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, patientColFamily);
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "pk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "patient_reports";
        patientColVal = "Has cold. No flu symptoms";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "pk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        try {
            accessor.dropColumnFamily(doctorColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, doctorColFamily);
        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports";

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace,
                queryColumnFamily, queryRowKey, queryColumnKey, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }
    
    @Test
    public void testGetColumnWithSixteenQueriesToCFInKVACPolicy() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicyForTest.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        try {
            accessor.dropColumnFamily(nurseColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, nurseColFamily);
        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        try {
            accessor.dropColumnFamily(patientColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, patientColFamily);
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "pk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "patient_reports_16";
        patientColVal = "Has cold. No flu symptoms";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "pk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        try {
            accessor.dropColumnFamily(doctorColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, doctorColFamily);
        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports_16";

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace,
                queryColumnFamily, queryRowKey, queryColumnKey, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }   
    
    @Test
    public void testGetColumnWith32QueriesToCFInKVACPolicy() throws Exception {
        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/PatientInfoSystemPolicyForTest.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        try {
            accessor.dropColumnFamily(nurseColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, nurseColFamily);
        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        try {
            accessor.dropColumnFamily(patientColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, patientColFamily);
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "pk";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        patientColkey = "patient_reports_32";
        patientColVal = "Has cold. No flu symptoms";
        accessor.put(keyspace, patientColFamily, patientRowkey, patientColkey,
            patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "pk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        try {
            accessor.dropColumnFamily(doctorColFamily);
        } catch (InvalidRequestException invalidRequest) {
            log.debug(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, doctorColFamily);
        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports_32";

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            String colValue = (String) accessor.get(keyspace,
                queryColumnFamily, queryRowKey, queryColumnKey, 1, null);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            log.debug("Column Value:" + colValue);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.debug("Avg time:" + avgTime);
    }        
}

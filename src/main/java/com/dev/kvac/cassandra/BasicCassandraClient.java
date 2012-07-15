package com.dev.kvac.cassandra;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicCassandraClient {

    private static Logger logger = LoggerFactory
        .getLogger(BasicCassandraClient.class);

    static BasicCassandraClient cassandraClient;

    private CassandraUtil cassandraUtil;
    static String keyspace;
    static int numberOfExperiments = 15;

    public BasicCassandraClient(String policyFilePath, String user,
        String password, String keyspace, String server, int port) {
        this.keyspace = keyspace;
        this.cassandraUtil = new CassandraUtil(user, password, keyspace);
        this.cassandraUtil.connect(server, port);
    }

    public static void main(String args[]) throws Exception {
        logger.info("Cassandra Client");

        String user = "kulkarni5";
        String password = "kulkarni5";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9170;

        String policyFilePath = "src/main/resources/Policy.xml";
        cassandraClient = new BasicCassandraClient(policyFilePath, user,
            password, keyspace, server, port);

        testGetWith1QueriesToCF();
        // testGetWith2QueriesToCF();
        // testGetColumnWithFiveQueriesToCFInKVACPolicy();
        // testGetColumnWithTenQueriesToCFInKVACPolicy();
        // testGetColumnWith20QueriesToCFInKVACPolicy();
        //testGetColumnWith40QueriesToCFInKVACPolicy();
    }
    
    public static void testGetWith1QueriesToCF() throws Exception {
   
        String columnFamilyPatient = "Patient";
        String rowKeyPatient = "jack";
        String columnKeyPatient = "medical_history";
        String columnValuePatient = "Cough";
      
        cassandraClient.put(keyspace, columnFamilyPatient, rowKeyPatient,
            columnKeyPatient, columnValuePatient, 1);

        String columnFamilyNurse = "Nurse";
        String rowKeyNurse = "kulkarni5";
        String columnKeyNurse = "work_hours";

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
        DateTime startDate = date.minusDays(2);
        DateTime endDate = date.plusDays(1);

        String startWorkHour = dateFormat.format(startDate.toDate());
        String endWorkHour = dateFormat.format(endDate.toDate());
        String columnValueNurse = startWorkHour + "-" + endWorkHour;
     
        cassandraClient.put(keyspace, columnFamilyNurse, rowKeyNurse, columnKeyNurse,
            columnValueNurse, 1);
        
        doGet(columnFamilyPatient, rowKeyPatient, columnKeyPatient);
    }    

    public static void testGetWith2QueriesToCF() throws Exception {

        String columnFamily = "Patient";
        String rowKey = "karve";
        String columnKey = "name";
        String value = "karve";
        long timestamp = 0;

        cassandraClient.put(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);

        columnFamily = "Doctor";
        rowKey = "kulkarni2";
        columnKey = "curr_patients";
        value = "karve";

        cassandraClient.put(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);

        String queryColumnFamily = "Patient";
        String queryRowKey = "karve";
        String queryColumnKey = "name";

        doGet(queryColumnFamily, queryRowKey, queryColumnKey);
    }

    public static void testGetColumnWithFiveQueriesToCFInKVACPolicy()
        throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "kulkarni2";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        cassandraClient.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "jack";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "dk";
        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "patient_reports";
        patientColVal = "Has cold. No flu symptoms. But he should be okay.";
        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "dk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        cassandraClient.put(keyspace, doctorColFamily, doctorRowkey,
            doctorColkey, doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "jack";
        String queryColumnKey = "patient_reports";

        doGet(queryColumnFamily, queryRowKey, queryColumnKey);
    }

    public static void testGetColumnWithTenQueriesToCFInKVACPolicy()
        throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "kulkarni1";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        cassandraClient.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "jack 110";
        String patientColkey = "location";
        String patientColVal = "ward-1";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "dk";
        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "patient_reports_10";
        patientColVal = "Has cold. No flu symptoms. But he should be okay. 10";
        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "dk";
        String doctorColkey = "location";
        String doctorColVal = "ward-1";

        cassandraClient.put(keyspace, doctorColFamily, doctorRowkey,
            doctorColkey, doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "jack 110";
        String queryColumnKey = "patient_reports_10";

        doGet(queryColumnFamily, queryRowKey, queryColumnKey);
    }

    public static void testGetColumnWith20QueriesToCFInKVACPolicy()
        throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "kulkarni";
        String nurseColkey = "location";
        String nurseColVal = "ward-3";

        cassandraClient.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "jack3 20";
        String patientColkey = "location";
        String patientColVal = "ward-3";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "dk3";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "patient_reports_20";
        patientColVal = "Has cold. No flu symptoms. But he should be okay. 20";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "dk3";
        String doctorColkey = "location";
        String doctorColVal = "ward-3";

        cassandraClient.put(keyspace, doctorColFamily, doctorRowkey,
            doctorColkey, doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "jack3 20";
        String queryColumnKey = "patient_reports_20";

        doGet(queryColumnFamily, queryRowKey, queryColumnKey);
    }

    public static void testGetColumnWith40QueriesToCFInKVACPolicy()
        throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "kulkarni4";
        String nurseColkey = "location";
        String nurseColVal = "ward-3";

        cassandraClient.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "jack3 40";
        String patientColkey = "location";
        String patientColVal = "ward-3";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "curr_doctor";
        patientColVal = "dk3";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        patientColkey = "patient_reports_40";
        patientColVal = "Has cold. No flu symptoms. But he should be okay. 40";

        cassandraClient.put(keyspace, patientColFamily, patientRowkey,
            patientColkey, patientColVal, 1);

        String doctorColFamily = "Doctor";
        String doctorRowkey = "dk3";
        String doctorColkey = "location";
        String doctorColVal = "ward-3";

        cassandraClient.put(keyspace, doctorColFamily, doctorRowkey,
            doctorColkey, doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "jack3 40";
        String queryColumnKey = "patient_reports_40";

        doGet(queryColumnFamily, queryRowKey, queryColumnKey);
    }

    private static void doGet(String queryColumnFamily, String queryRowKey,
        String queryColumnKey) throws Exception {
        double totalTime = 0;
        long[] times = new long[numberOfExperiments];
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            Object colValue = cassandraClient.get(keyspace, queryColumnFamily,
                queryRowKey, queryColumnKey, 1);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            times[i] = (end - start);
            logger.info("Time: " + (end - start));
            logger.info("Column Value:" + new String((byte[]) colValue));
        }
        logger.info("Total time:" + totalTime);
        Double avgTime = totalTime / numberOfExperiments;
        DecimalFormat df = new DecimalFormat("#.##");
        logger.info("Avg time:" + df.format(avgTime));
        Arrays.sort(times);
        long median = times[numberOfExperiments / 2];
        logger.info("Median time:" + median);
    }

    public Object get(String keyspace, String columnFamily, String rowKey,
        String columnKey, long timestamp) throws Exception {
        return cassandraUtil.get(columnFamily, rowKey, columnKey);
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, Object value, long timestamp) throws Exception {
        cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);
    }

    public void delete(String columnFamily, String rowKey, String column)
        throws Exception {
        cassandraUtil.delete(columnFamily, rowKey, column);
    }
}
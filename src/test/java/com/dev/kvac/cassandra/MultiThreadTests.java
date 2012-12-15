package com.dev.kvac.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadTests {

    private final Logger log = LoggerFactory.getLogger(MultiThreadTests.class);

    // Setup and initiate Poller thread executor
    private ScheduledExecutorService accessorThreadExecutor = Executors
        .newSingleThreadScheduledExecutor();
    int numberOfThreads = 500;
    ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);

    public static void main(String[] args) throws Exception {
        MultiThreadTests driver = new MultiThreadTests();
        driver.execute();
    }

    public void execute() throws Exception {

        List<Future<Object>> futureResults = new ArrayList<Future<Object>>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            futureResults.add(service.submit(new CassandraIO<Future<Object>>(""
                + i)));
        }

        List<Object> resultArray = new ArrayList<Object>();
        for (Future<?> f : futureResults) {
            String intermediateResult = (String) f.get();
            resultArray.addAll(Arrays.asList(intermediateResult));
        }
        long end = System.currentTimeMillis();
        log.info("Total time for executing " + numberOfThreads + " threads is:"
            + (end - start) + " ms.");
        log.info("Result data size:" + resultArray.size());
        log.info("Result data:" + resultArray.toString());

        Assert.assertEquals(numberOfThreads, resultArray.size());
    }

    public ScheduledExecutorService getPollerThreadExecutor() {
        return accessorThreadExecutor;
    }
}

class CassandraIO<T> implements Callable<Object> {

    private Logger log = LoggerFactory.getLogger(CassandraIO.class);
    private CassandraAccessor accessor;
    private String user = "devdatta";
    String password = "devdatta";
    String keyspace = "PatientInfoSystem";
    String server = "localhost";
    int port = 9160;
    String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
    String threadNumber;
    int numberOfExperiments = 1;

    public CassandraIO(String patientNumber) throws Exception {
        this.threadNumber = patientNumber;
        accessor = new CassandraAccessor(policyFilePath, user, password,
            keyspace, server, port);
    }

    public Object call() throws Exception {

        String nurseColFamily = "Nurse";
        String nurseRowkey = "devdatta";
        String nurseColkey = "location";
        String nurseColVal = "ward-1";

        accessor.put(keyspace, nurseColFamily, nurseRowkey, nurseColkey,
            nurseColVal, 1);

        String patientColFamily = "Patient";
        String patientRowkey = "john";
        String patientColkey = "location";
        String patientColVal = "ward-1";

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

        accessor.put(keyspace, doctorColFamily, doctorRowkey, doctorColkey,
            doctorColVal, 1);

        String queryColumnFamily = "Patient";
        String queryRowKey = "john";
        String queryColumnKey = "patient_reports";
        long totalTime = 0;
        String colValue = null;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            colValue = (String) accessor.get(keyspace, queryColumnFamily,
                queryRowKey, queryColumnKey, 1, null);
            log.info("Column Value:" + colValue);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
        }
        double avgTime = totalTime / numberOfExperiments;
        log.info("Avg time:" + avgTime);

        return colValue;
    }
}
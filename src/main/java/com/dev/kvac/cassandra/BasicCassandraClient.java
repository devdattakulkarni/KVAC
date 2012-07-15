package com.dev.kvac.cassandra;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicCassandraClient {

    private static Logger logger = LoggerFactory
        .getLogger(BasicCassandraClient.class);

    static BasicCassandraClient cassandraClient;

    private CassandraUtil cassandraUtil;
    static String keyspace;
    static int numberOfExperiments = 10;

    public BasicCassandraClient(String policyFilePath, String user,
        String password, String keyspace, String server, int port) {
        this.keyspace = keyspace;
        this.cassandraUtil = new CassandraUtil(user, password, keyspace);
        this.cassandraUtil.connect(server, port);
    }

    public static void main(String args[]) throws Exception {
        logger.info("Cassandra Client");

        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/Policy.xml";
        cassandraClient = new BasicCassandraClient(policyFilePath, user,
            password, keyspace, server, port);

        testGetWith2QueriesToCF();
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
        rowKey = "devdatta";
        columnKey = "curr_patients";
        value = "karve";

        cassandraClient.put(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);

        String queryColumnFamily = "Patient";
        String queryRowKey = "karve";
        String queryColumnKey = "name";

        long totalTime = 0;
        for (int i = 0; i < numberOfExperiments; i++) {
            long start = System.currentTimeMillis();
            Object colValue = cassandraClient.get(keyspace, queryColumnFamily,
                queryRowKey, queryColumnKey, timestamp);
            long end = System.currentTimeMillis();
            totalTime += (end - start);
            logger.info("Column value:" + new String((byte[]) colValue));
        }
        double avgTime = totalTime / numberOfExperiments;
        logger.info("Avg Time: " + avgTime);
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

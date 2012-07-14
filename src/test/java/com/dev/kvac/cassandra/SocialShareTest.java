package com.dev.kvac.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.joda.time.DateTime;
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
    public void testPolicy1_ARowAccessPolicy() throws Exception {
        String keyspace = "SocialShare";
        CassandraAccessor accessor = getCassandraAccessor(keyspace);

        String columnFamily = "Person";
        String rowKey = "jr";
        String columnKey = "name";
        String columnValue = "jr";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            System.out.println(invalidRequest.getMessage());
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
        String queryColumnKey = "name";

        String colValue = (String)accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1, null);
        System.out.println("Column Value:{" + colValue + "}");
        List<String> expectedList = new ArrayList<String>();
        expectedList.add("name:jr");
        expectedList.add("family:devdatta");
        expectedList.add("plans:Visit China");
        StringTokenizer st = new StringTokenizer(colValue, "|");
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (expectedList.contains(token)) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        }

        rowKey = "purandare";
        columnKey = "family";
        columnValue = "aai purandare";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        queryRowKey = "purandare";
        queryColumnKey = "plans";
        colValue = (String)accessor.get(keyspace, queryColumnFamily, queryRowKey,
            queryColumnKey, 1, null);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertNull(plans, colValue);
    }

    @Test
    public void testPolicy2_AColumnAccessPolicy() throws Exception {
        String keyspace = "SocialShare";
        CassandraAccessor accessor = getCassandraAccessor(keyspace);

        String columnFamily = "Person";
        String rowKey = "jr";
        String columnKey = "name";
        String columnValue = "jr";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            System.out.println(invalidRequest.getMessage());
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

        String colValue = (String)accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1, null);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertEquals(plans, colValue);

        rowKey = "purandare";
        columnKey = "family";
        columnValue = "aai purandare";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        queryRowKey = "purandare";
        queryColumnKey = "plans";
        colValue = (String)accessor.get(keyspace, queryColumnFamily, queryRowKey,
            queryColumnKey, 1, null);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertNull(plans, colValue);

    }

    @Test
    public void testPolicy3_AColumnAccessPolicy() throws Exception {
        String keyspace = "SocialShare";
        CassandraAccessor accessor = getCassandraAccessor(keyspace);

        String columnFamily = "Person";
        String rowKey = "pk";
        String columnKey = "name";
        String columnValue = "pk";

        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            System.out.println(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnKey = "friend";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        rowKey = "devdatta";
        columnKey = "name";
        columnValue = "devdatta";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        rowKey = "pk";
        columnKey = "messageId";
        String messages = "1,2,3";
        accessor.put(keyspace, columnFamily, rowKey, columnKey, messages, 1);

        columnFamily = "Messages";
        try {
            accessor.dropColumnFamily(columnFamily);
        } catch (InvalidRequestException invalidRequest) {
            System.out.println(invalidRequest.getMessage());
        }
        accessor.addColumnFamily(keyspace, columnFamily);

        columnKey = "message_time_stamp";

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

        // DateTimeFormatter dateFormat = ISODateTimeFormat.dateTime();

        DateTime date = new DateTime();
        DateTime d1 = date.minusDays(2);
        DateTime d2 = date.plusDays(1);
        DateTime d3 = date;

        rowKey = "1";
        columnValue = dateFormat.format(d1.toDate());
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        rowKey = "2";
        columnValue = dateFormat.format(d2.toDate());
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        rowKey = "3";
        columnValue = dateFormat.format(d2.toDate());
        accessor.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String queryColumnFamily = "Person";
        String queryRowKey = "pk";
        String queryColumnKey = "messageId";

        Map<String, String> runtimeParams = new HashMap<String, String>();
        runtimeParams.put("$c", "1");
        String colValue = (String)accessor.get(keyspace, queryColumnFamily,
            queryRowKey, queryColumnKey, 1, runtimeParams);
        System.out.println("Column Value:{" + colValue + "}");

        Assert.assertTrue(colValue.contains("1"));

        runtimeParams.put("$c", "3");
        colValue = (String)accessor.get(keyspace, queryColumnFamily, queryRowKey,
            queryColumnKey, 1, runtimeParams);
        System.out.println("Column Value:{" + colValue + "}");
        Assert.assertNull(colValue);

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

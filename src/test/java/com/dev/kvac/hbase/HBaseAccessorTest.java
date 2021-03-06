package com.dev.kvac.hbase;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.TableNotDisabledException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseAccessorTest {

    private static Logger logger = LoggerFactory.getLogger(HBaseAccessorTest.class);
    HBaseAccessor hbase;

    @Before
    public void setup() {
    }

    @Test
    public void testGetSuccess() throws Exception {
        logger.info("HBase Client");
        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);

        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";

        List<String> colFamilies = new ArrayList<String>();
        colFamilies.add(columnFamily);
        colFamilies.add("Patient");
        setupTableAndColumnFamilies(keyspace, colFamilies);

        Long t1 = new Long(1);
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, t1
            .longValue());

        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        columnValue = "jack";

        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, t1
            .longValue());

        String colValue = (String)hbase.get(keyspace, columnFamily, rowKey, columnKey,
            t1.longValue(), null);
        logger.info("Column Value:" + colValue);
        Assert.assertEquals(columnValue, colValue);
    }

    @Test
    public void testGetFailure() throws Exception {
        logger.info("HBase Client");
        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);

        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";

        List<String> colFamilies = new ArrayList<String>();
        colFamilies.add(columnFamily);
        colFamilies.add("Patient");
        setupTableAndColumnFamilies(keyspace, colFamilies);

        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        columnFamily = "Patient";
        rowKey = "john";
        columnKey = "name";
        columnValue = "john";

        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, 1);

        String colValue = (String)hbase.get(keyspace, columnFamily, rowKey, columnKey,
            1, null);
        logger.info("Column Value:" + colValue);
        Assert.assertNull(colValue);
    }

    @Test
    public void testMultipleValuesInAColumn() throws Exception {
        logger.info("HBase Client");
        String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";
        String user = "devdatta";
        hbase = new HBaseAccessor(policyFilePath, user);

        String keyspace = "PatientInfoSystem";
        String columnFamily = "Doctor";
        String rowKey = "devdatta";
        String columnKey = "curr_patients";
        String columnValue = "jack";

        List<String> colFamilies = new ArrayList<String>();
        colFamilies.add(columnFamily);
        colFamilies.add("Patient");
        setupTableAndColumnFamilies(keyspace, colFamilies);

        Long t1 = new Long(1);
        Long t2 = new Long(2);
        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, t1
            .longValue());

        columnFamily = "Patient";
        rowKey = "jack";
        columnKey = "name";
        columnValue = "jack";

        hbase.put(keyspace, columnFamily, rowKey, columnKey, columnValue, t1
            .longValue());

        hbase.put(keyspace, columnFamily, rowKey, columnKey, "jill", t2
            .longValue());

        String colValue = (String)hbase.get(keyspace, columnFamily, rowKey, columnKey,
            t1.longValue(), null);
        logger.debug("Column Value: {}",colValue);
        Assert.assertEquals(columnValue, colValue);

        colValue = (String)hbase.get(keyspace, columnFamily, rowKey, columnKey, t2
            .longValue(), null);
        logger.debug("Column Value: {}",colValue);
        Assert.assertEquals("jill", colValue);
    }

    private void setupTableAndColumnFamilies(String tableName,
        List<String> columnFamilies) {
        try {
            HBaseUtil.disableTable(tableName);
            HBaseUtil.deleteTable(tableName);
            HBaseUtil.addTable(tableName, columnFamilies);
        } catch (TableNotDisabledException exp) {
            exp.printStackTrace();
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}

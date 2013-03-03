package com.dev.kvac.cassandra;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;

public class CassandraAccessor implements KVStoreInterface {

    private static Logger logger = LoggerFactory
        .getLogger(CassandraAccessor.class);
    private Map<Node, Node> resourcePolicyMap;
    private String user;
    private Evaluator evaluator;
    private CassandraUtil cassandraUtil;
    private Map<String, String> runtimeParams;

    public CassandraAccessor(String policyFilePath, String user,
        String password, String keyspace, String server, int port)
        throws Exception {
        this.user = user;
        this.resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
        this.evaluator = new CassandraEvaluator(this, "cassandra");
        this.runtimeParams = new HashMap<String, String>();

        this.cassandraUtil = new CassandraUtil(user, password, keyspace);
        this.cassandraUtil.connect(server, port);
    }

    public Map<Node, Node> getResourcePolicyMap() {
        return resourcePolicyMap;
    }

    public CassandraUtil getCassandraUtil() {
        return cassandraUtil;
    }

    public synchronized Object get(String keyspace, String columnFamily,
        String rowKey, String columnKey, long timestamp,
        Map<String, String> runtimeParams) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;

        this.runtimeParams = runtimeParams;

        //if (logger.isDebugEnabled()) {
        //    logger.debug("Resource:" + resource);
        //}

        Object[] resAndPermisison = KVACUtil.findPermissionNodeForResource(
            resourcePolicyMap, resource);

        String resourceType = (String) resAndPermisison[0];
        Node permissionNode = (Node) resAndPermisison[1];

        String requestedPermission = "read"; // "get" is "read"
        long startTime = System.currentTimeMillis();
        boolean result = this.evaluator.evaluate(rowKey, permissionNode,
            requestedPermission);
        long endTime = System.currentTimeMillis();

        //if (logger.isDebugEnabled()) {
        //    logger.debug("Total time:" + (endTime - startTime));
        //}

        String value = null;
        if (result) {
            byte[] val;
            if (resourceType.equalsIgnoreCase("column")) {
                val = (byte[]) cassandraUtil.get(columnFamily, rowKey,
                    columnKey);
                //BufferedInputStream buffer = new BufferedInputStream(
                //    new ByteArrayInputStream(val));
                //ObjectInput input = new ObjectInputStream(buffer);
                value = new String(val); //(String) input.readObject();
            } else if (resourceType.equalsIgnoreCase("row")) {
                value = cassandraUtil.getRow(columnFamily, rowKey);
            }
        }
        return value;
    }
    
    /*
    public List<String> getAllCols() throws Exception {
    	List<ColumnOrSuperColumn> get_slice(ByteBuffer key,
    			ColumnParent column_parent, SlicePredicate predicate,
    			ConsistencyLevel consistency_level)
    } */
    
    public String getRow(String columnFamily, String rowKey) throws Exception {
    	String retVal = cassandraUtil.getRow(columnFamily, rowKey);
    	return retVal;
    }

    public String direct_get(String keyspace, String columnFamily,
        String rowKey, String columnKey, long timestamp) throws Exception {
        byte [] obj = (byte [])cassandraUtil.get(columnFamily, rowKey, columnKey);
        return new String(obj);
    }
    
    public Map<String,String> get_versions(String keyspace, String columnFamily,
            String rowKey, String columnKey, long timestamp) throws Exception {
            Map<String,String> vmap = cassandraUtil.get_super_col(columnFamily, rowKey, columnKey);
            return vmap;
        }

    public synchronized void put(String keyspace, String columnFamily,
        String rowKey, String columnKey, Object value, long timestamp)
        throws Exception {
        cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);
    }
    
    public synchronized void put_with_super_col(String keyspace, String columnFamily,
            String rowKey, String columnKey, String supercolumn, Object value, long timestamp)
            throws Exception {
		cassandraUtil.add_with_super_col(keyspace, columnFamily, rowKey,
				columnKey, supercolumn, value, timestamp);            
        }

    public void delete(String columnFamily, String rowKey, String column)
        throws Exception {
        cassandraUtil.delete(columnFamily, rowKey, column);
    }

    public synchronized void dropColumnFamily(String columnFamily) throws Exception {
        cassandraUtil.dropColumnFamily(columnFamily);
    }

    public synchronized void addColumnFamily(String keyspace, String columnFamily)
        throws Exception {
        cassandraUtil.addColumnFamily(keyspace, columnFamily);
    }

    public String getUser() {
        return user;
    }

    public static void main(String args[]) throws Exception {
        logger.info("Cassandra Client");

        String user = "devdatta";
        String password = "devdatta";
        String keyspace = "PatientInfoSystem";
        String server = "localhost";
        int port = 9160;

        String policyFilePath = "src/main/resources/Policy.xml";
        CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
            user, password, keyspace, server, port);

        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "name";

        String colValue = (String) accessor.get(keyspace, columnFamily, rowKey,
            columnKey, System.currentTimeMillis(), null);
        logger.info("Column Value: {}", colValue);
    }

    public String getRuntimeParameterValues(String key) throws Exception {
        String val = runtimeParams.get(key);
        return val;
    }

}
package com.dev.kvac.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Node;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;

public class CassandraAccessor implements KVStoreInterface {

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
        this.evaluator = new Evaluator(this, "cassandra");
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

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey, long timestamp, Map<String,String> runtimeParams) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;
        
        this.runtimeParams = runtimeParams;

        System.out.println("Resource:" + resource);

        Object[] resAndPermisison = KVACUtil.findPermissionNodeForResource(
            resourcePolicyMap, resource);

        String resourceType = (String) resAndPermisison[0];
        Node permissionNode = (Node) resAndPermisison[1];

        String requestedPermission = "read"; // "get" is "read"
        boolean result = this.evaluator.evaluate(rowKey, permissionNode,
            requestedPermission);

        String value = null;
        if (result) {
            if (resourceType.equalsIgnoreCase("column")) {
                value = cassandraUtil.get(columnFamily, rowKey, columnKey);
            } else if (resourceType.equalsIgnoreCase("row")) {
                value = cassandraUtil.getRow(columnFamily, rowKey);
            }
        }

        return value;
    }

    public String direct_get(String keyspace, String columnFamily,
        String rowKey, String columnKey, long timestamp) throws Exception {
        return cassandraUtil.get(columnFamily, rowKey, columnKey);
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value, long timestamp) throws Exception {
        cassandraUtil.add(columnFamily, rowKey, columnKey, value, timestamp);
    }

    public void delete(String columnFamily, String rowKey, String column)
        throws Exception {
        cassandraUtil.delete(columnFamily, rowKey, column);
    }

    public void dropColumnFamily(String columnFamily) throws Exception {
        cassandraUtil.dropColumnFamily(columnFamily);
    }

    public void addColumnFamily(String keyspace, String columnFamily)
        throws Exception {
        cassandraUtil.addColumnFamily(keyspace, columnFamily);
    }

    public String getUser() {
        return user;
    }

    public static void main(String args[]) throws Exception {
        System.out.println("Cassandra Client");

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

        String colValue = accessor.get(keyspace, columnFamily, rowKey,
            columnKey, System.currentTimeMillis(), null);
        System.out.println("Column Value:" + colValue);
    }

    public String getRuntimeParameterValues(String key) throws Exception {
        String val = runtimeParams.get(key);
        return val;
    }

}
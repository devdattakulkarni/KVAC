package com.dev.kvac.cassandra;

import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.w3c.dom.Node;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;

public class CassandraAccessor implements KVStoreInterface {

    private Map<String, Node> resourcePolicyMap;
    private String user;
    private Evaluator evaluator;
    private CassandraUtil cassandraUtil;

    public CassandraAccessor(String policyFilePath, String user,
        String password, String keyspace, String server, int port)
        throws Exception {
        this.user = user;
        this.resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
        this.evaluator = new Evaluator(this, "cassandra");

        this.cassandraUtil = new CassandraUtil(user, password, keyspace);
        this.cassandraUtil.connect(server, port);
    }

    public CassandraUtil getCassandraUtil() {
        return cassandraUtil;
    }

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;

        System.out.println("Resource:" + resource);
        Node whereNode = resourcePolicyMap.get(resource);

        boolean result = this.evaluator.evaluate(rowKey, whereNode);

        String value = null;
        if (result) {
            value = cassandraUtil.get(columnFamily, rowKey, columnKey);
        }

        return value;
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value) throws Exception {
        cassandraUtil.add(columnFamily, rowKey, columnKey, value);
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
            columnKey);
        System.out.println("Column Value:" + colValue);
    }

}
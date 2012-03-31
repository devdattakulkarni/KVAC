package com.dev.kvac.hbase;

import java.util.Map;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;

import org.w3c.dom.Node;

public class HBaseAccessor implements KVStoreInterface {

    private Map<String, Node> resourcePolicyMap;
    private String user;
    private Evaluator evaluator;

    public HBaseAccessor(String policyFilePath, String user) throws Exception {
        resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
        this.user = user;
        this.evaluator = new Evaluator(this, "hbase");
    }

    public String getUser() {
        return user;
    }

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey, long timestamp) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;

        System.out.println("Resource:" + resource);
        Node whereNode = resourcePolicyMap.get(resource);

        String requestedPermission = "read";
        boolean result = this.evaluator.evaluate(rowKey, whereNode, requestedPermission);
        String value = null;
        if (result) {
            value = HBaseUtil.get(keyspace, rowKey, columnKey, timestamp);
        }
        return value;
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value, long timestamp) throws Exception {

        HBaseUtil.put(keyspace, columnFamily, rowKey, columnKey, value,
            timestamp);
    }

    public void clean(String keyspace, String rowKey) throws Exception {
        HBaseUtil.clean(keyspace, rowKey);
    }
}

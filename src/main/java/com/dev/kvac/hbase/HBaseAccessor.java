package com.dev.kvac.hbase;

import java.util.Map;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;

import org.w3c.dom.Node;

public class HBaseAccessor implements KVStoreInterface {

    private Map<Node, Node> resourcePolicyMap;
    private String user;
    private Evaluator evaluator;
    private Map<String, String> runtimeParams;

    public HBaseAccessor(String policyFilePath, String user) throws Exception {
        resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
        this.user = user;
        this.evaluator = new Evaluator(this, "hbase");
    }

    public String getUser() {
        return user;
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

        String requestedPermission = "read";
        boolean result = this.evaluator.evaluate(rowKey, permissionNode,
            requestedPermission);
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

    public String getRuntimeParameterValues(String key) throws Exception {
        return runtimeParams.get(key);
    }
}

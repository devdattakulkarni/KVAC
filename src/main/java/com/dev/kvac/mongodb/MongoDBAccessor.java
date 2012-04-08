package com.dev.kvac.mongodb;

import java.util.Map;

import org.w3c.dom.Node;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;
import com.dev.kvac.hbase.HBaseUtil;

public class MongoDBAccessor implements KVStoreInterface {

    private Map<Node, Node> resourcePolicyMap;
    String user;
    Evaluator evaluator;
    MongoDBUtil mongoUtil;
    private Map<String,String> runtimeParams;

    public MongoDBAccessor(String host, int port, String user,
        String policyFilePath) throws Exception {
        this.user = user;
        this.evaluator = new Evaluator(this, "mongodb");
        this.mongoUtil = new MongoDBUtil(host, port);
        resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
    }

    public Object get(String keyspace, String columnFamily, String rowKey,
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
            value = mongoUtil.get(keyspace, columnFamily, rowKey, columnKey);
        }
        return value;
    }

    public String getUser() {
        return user;
    }

    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, Object value, long timestamp) throws Exception {
        mongoUtil.put(keyspace, columnFamily, rowKey, columnKey, (String)value);
    }

    public void clean(String keyspace) throws Exception {
        mongoUtil.clean(keyspace);
    }

    public MongoDBUtil getUtil() {
        return mongoUtil;
    }

    public String getRuntimeParameterValues(String key) throws Exception {
        return runtimeParams.get(key);
    }

}

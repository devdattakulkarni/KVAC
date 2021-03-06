package com.dev.kvac.hbase;

import java.util.Map;

import com.dev.kvac.Evaluator;
import com.dev.kvac.KVACUtil;
import com.dev.kvac.KVStoreInterface;
import com.dev.kvac.cassandra.CassandraEvaluator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class HBaseAccessor implements KVStoreInterface {

    private static Logger logger = LoggerFactory.getLogger(HBaseEvaluator.class);
    private Map<Node, Node> resourcePolicyMap;
    private String user;
    private Evaluator evaluator;
    private Map<String, String> runtimeParams;

    public HBaseAccessor(String policyFilePath, String user) throws Exception {
        resourcePolicyMap = KVACUtil.readPolicyFile(policyFilePath);
        this.user = user;
        this.evaluator = new HBaseEvaluator(this, "hbase");
    }

    public String getUser() {
        return user;
    }

    public Object get(String keyspace, String columnFamily, String rowKey,
        String columnKey, long timestamp, Map<String,String> runtimeParams) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;
        
        this.runtimeParams = runtimeParams;

        logger.debug("Resource: {}",resource);
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
        String columnKey, Object value, long timestamp) throws Exception {

        HBaseUtil.put(keyspace, columnFamily, rowKey, columnKey, (String)value,
            timestamp);
    }

    public void clean(String keyspace, String rowKey) throws Exception {
        HBaseUtil.clean(keyspace, rowKey);
    }

    public String getRuntimeParameterValues(String key) throws Exception {
        return runtimeParams.get(key);
    }
}

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
        String columnKey) throws Exception {

        String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;

        System.out.println("Resource:" + resource);
        Node whereNode = resourcePolicyMap.get(resource);

        boolean result = this.evaluator.evaluate(rowKey, whereNode);

        String value = null;
        if (result) {
            value = HBaseUtil.get(keyspace, rowKey, columnKey);
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("HBase Client");
        String policyFilePath = "src/main/resources/Policy.xml";
        String user = "devdatta";
        HBaseAccessor hbase = new HBaseAccessor(policyFilePath, user);
        String keyspace = "PatientInfoSystem";
        String columnFamily = "Patient";
        String rowKey = "john";
        String columnKey = "name";
        String colValue = hbase.get(keyspace, columnFamily, rowKey, columnKey);
        System.out.println("Column Value:" + colValue);
    }
}

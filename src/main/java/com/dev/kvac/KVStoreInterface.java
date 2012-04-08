package com.dev.kvac;

import java.util.Map;

public interface KVStoreInterface {

    public Object get(String keyspace, String columnFamily, String rowKey,
        String columnKey, long timestamp, Map<String,String> runtimeParams) throws Exception;
    
    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, Object value, long timestamp) throws Exception;
    
    public String getUser();
    
    public String getRuntimeParameterValues(String key) throws Exception;

}

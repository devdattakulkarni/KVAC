package com.dev.kvac;

public interface KVStoreInterface {

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey) throws Exception;
    
    public void put(String keyspace, String columnFamily, String rowKey,
        String columnKey, String value) throws Exception;

    public String getUser();

}

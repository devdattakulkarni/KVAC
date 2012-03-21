package com.dev.kvac;

public interface KVStoreInterface {

    public String get(String keyspace, String columnFamily, String rowKey,
        String columnKey) throws Exception;

    public String getUser();

}

package com.dev.kvac.cassandra;

public interface ProvenanceDao {
	
	void insert(String resourceKey, String accessor, String operation) throws Exception ;

}

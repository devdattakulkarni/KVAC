package com.dev.kvac.cassandra;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientInfoSystemTAPPExperiments {

	private CassandraAccessor accessor;
	private final Logger log = LoggerFactory
			.getLogger(PatientInfoSystemCODASPYExperimentsKVACINCassandra.class);
	String user = "devdatta";
	String password = "devdatta";
	String keyspace = "PatientInfoSystem";
	String server = "localhost";
	int port = 9170;

	String policyFilePath = "src/main/resources/PatientInfoSystemPolicy.xml";

	@Before
	public void setup() throws Exception {
		accessor = new CassandraAccessor(policyFilePath, user, password,
				keyspace, server, port);
	}

	@Test
	public void insert_multiple_versions_for_a_column() throws Exception {
		String columnFamily = "Nurse";
		String rowKey = "JodhaAkbar";
		String supercolumn = "medication";
		long timestamp = 22;
		String value = "Aspro 22";

		long start = System.currentTimeMillis();
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			String columnKey = Long.toString(timestamp);
			accessor.put_with_super_col(keyspace, columnFamily, rowKey,
					columnKey, supercolumn, value, timestamp);
		}
	}
	
	@Test
	public void get_multiple_versions_for_a_column() throws Exception {
		String columnFamily = "Nurse";
		String rowKey = "JodhaAkbar";
		String supercolumn = "medication";
		long timestamp = 21;
		
		Map<String,String> vmap = accessor.get_versions(keyspace, columnFamily, rowKey, supercolumn, timestamp);
		
		for(Entry<String,String> version : vmap.entrySet()) {
			System.out.println(version.getKey() + " " + version.getValue());
		}		
	}

	@Test
	public void insert10kKeys() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "JodhaAkbar";
		String columnKey = "medication";
		long timestamp = 23;
		String value = "Aspro 23";

		long start = System.currentTimeMillis();
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.put(keyspace, columnFamily, rowKey, columnKey, value + " "
					+ i, timestamp);
		}

		long end = System.currentTimeMillis();
		long totalTime = end - start;
		double avgInsertTimePerRecord = totalTime / dataSize;
		log.debug("Total insertion time:" + totalTime);
		log.debug("Per record insertion time:" + avgInsertTimePerRecord);
	}

	@Test
	public void testReadProvenance() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "JodhaAkbar";
		String columnKey = "medication";
		long timestamp = 23;

		long start = System.currentTimeMillis();
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			String result = accessor.direct_get(keyspace, columnFamily, rowKey,
					columnKey, timestamp);
			log.debug("Got value:" + result);
		}

		long end = System.currentTimeMillis();
		long totalTime = end - start;
		double avgInsertTimePerRecord = totalTime / dataSize;
		log.debug("Total read time:" + totalTime);
		log.debug("Per record read time:" + avgInsertTimePerRecord);
	}

	@Test
	public void provenanceQuery_ALL() throws Exception {

		String columnFamily = "Provenance";
		String rowKey = "/PatientInfoSystem/Patient(id=Jodha)/medication:enable_dataprov_write";

		// accessor.get(keyspace, columnFamily, rowKey, columnKey, timestamp,
		// runtimeParams);

	}
}
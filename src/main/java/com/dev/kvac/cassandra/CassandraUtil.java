package com.dev.kvac.cassandra;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.cli.CliClient;
import org.apache.cassandra.cli.CliSessionState;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraUtil {

    private static TTransport transport = null;
    private static Cassandra.Client thriftClient = null;
    public static CliSessionState sessionState = null;
    private static CliClient cliClient;

    public CassandraUtil(String user, String password, String keyspace) {
        sessionState = new CliSessionState();
        sessionState.username = user;
        sessionState.password = password;
        sessionState.keyspace = keyspace;
        sessionState.debug = true;
    }

    public Cassandra.Client getThriftClient() {
        return thriftClient;
    }

    public void connect(String server, int port) {

        TSocket socket = new TSocket(server, port);

        if (transport != null)
            transport.close();

        if (sessionState.framed) {
            transport = new TFramedTransport(socket);
        } else {
            transport = socket;
        }

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport, true,
            true);
        Cassandra.Client cassandraClient = new Cassandra.Client(binaryProtocol);

        try {
            transport.open();
        } catch (Exception e) {
            if (sessionState.debug)
                e.printStackTrace();
            String error = (e.getCause() == null) ? e.getMessage() : e
                .getCause().getMessage();
            throw new RuntimeException("Exception connecting to " + server
                + "/" + port + ". Reason: " + error + ".");
        }

        thriftClient = cassandraClient;
        cliClient = new CliClient(sessionState, thriftClient);

        if ((sessionState.username != null) && (sessionState.password != null)) {
            // Authenticate
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(SimpleAuthenticator.USERNAME_KEY,
                sessionState.username);
            credentials.put(SimpleAuthenticator.PASSWORD_KEY,
                sessionState.password);
            AuthenticationRequest authRequest = new AuthenticationRequest(
                credentials);
            try {
                thriftClient.login(authRequest);
                cliClient.setUsername(sessionState.username);
            } catch (AuthenticationException e) {
                thriftClient = null;
                sessionState.err
                    .println("Exception during authentication to the cassandra node, "
                        + "Verify the keyspace exists, and that you are using the correct credentials.");
                return;
            } catch (AuthorizationException e) {
                thriftClient = null;
                sessionState.err
                    .println("You are not authorized to use keyspace: "
                        + sessionState.keyspace);
                return;
            } catch (TException e) {
                thriftClient = null;
                sessionState.err
                    .println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
                return;
            }
        }

        String clusterName;
        try {
            clusterName = thriftClient.describe_cluster_name();
            thriftClient.set_keyspace(sessionState.keyspace);
        } catch (Exception e) {
            sessionState.err
                .println("Exception retrieving information about the cassandra node, check you have connected to the thrift port.");
            if (sessionState.debug) {
                e.printStackTrace();
            }
            return;
        }
        sessionState.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName,
            server, port);
    }

    public String get(String columnFamily, String rowKey, String column)
        throws Exception {
        ByteBuffer keyOfAccessor = ByteBuffer.allocate(6);
        // String t1 = "jsmith";
        byte[] t1array = rowKey.getBytes(Charset.forName("ISO-8859-1"));
        keyOfAccessor = ByteBuffer.wrap(t1array);

        // 2.2 Create the ColumnPath
        ColumnPath accessorColPath = new ColumnPath();
        accessorColPath.setColumn_family(columnFamily);
        accessorColPath.setColumn(column.getBytes());

        Column retColumn;
        try {
            ConsistencyLevel consistency_level = ConsistencyLevel
                .findByValue(1);
            retColumn = thriftClient.get(keyOfAccessor, accessorColPath,
                consistency_level).column;

        } catch (NotFoundException e) {
            e.printStackTrace();
            return null;
        }

        byte[] columnValue = retColumn.getValue();
        String value = new String(columnValue);
        // System.out.println("Got Value:" + value);
        return value;
    }

    public String getRow(String columnFamily, String rowKey) throws Exception {
        ByteBuffer keyOfAccessor = ByteBuffer.allocate(6);
        byte[] keyOfAccessorArray = rowKey.getBytes(Charset
            .forName("ISO-8859-1"));
        keyOfAccessor = ByteBuffer.wrap(keyOfAccessorArray);

        List<KeySlice> keySlice;

        ConsistencyLevel consistency_level = ConsistencyLevel.findByValue(1);

        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(keyOfAccessor);
        keyRange.setEnd_key(keyOfAccessor);

        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(columnFamily);

        SlicePredicate predicate = new SlicePredicate();
        SliceRange slice_range = new SliceRange();
        slice_range.setStart("".getBytes());
        slice_range.setFinish("".getBytes());

        predicate.setSlice_range(slice_range);

        keySlice = thriftClient.get_range_slices(columnParent, predicate,
            keyRange, consistency_level);

        StringBuffer colNameColValue = new StringBuffer();
        for (int i = 0; i < keySlice.size(); i++) {
            KeySlice slice = keySlice.get(i);
            List<ColumnOrSuperColumn> cols = slice.getColumns();
            for (int j = 0; j < cols.size(); j++) {
                ColumnOrSuperColumn c = cols.get(j);
                colNameColValue.append(new String(c.getColumn().getName()));
                colNameColValue.append(":"
                    + new String(c.getColumn().getValue()) + "  ");
            }
        }

        return colNameColValue.toString();
    }

    public void add(String columnFamily, String rowKey, String column,
        String value, long timestamp) throws Exception {
        ByteBuffer keyOfAccessor = ByteBuffer.allocate(6);

        byte[] t1array = rowKey.getBytes(Charset.forName("ISO-8859-1"));
        keyOfAccessor = ByteBuffer.wrap(t1array);

        ColumnParent colParent = new ColumnParent();
        colParent.setColumn_family(columnFamily);
        Column col = new Column();
        col.setName(column.getBytes());
        col.setValue(value.getBytes());
        col.setTimestamp(timestamp);

        ConsistencyLevel consistency_level = ConsistencyLevel.findByValue(1);
        thriftClient.insert(keyOfAccessor, colParent, col, consistency_level);

    }

    public void delete(String columnFamily, String rowKey, String column)
        throws Exception {
        ByteBuffer keyOfAccessor = ByteBuffer.allocate(6);
        // String t1 = "jsmith";
        byte[] t1array = rowKey.getBytes(Charset.forName("ISO-8859-1"));
        keyOfAccessor = ByteBuffer.wrap(t1array);

        // 2.2 Create the ColumnPath
        ColumnPath accessorColPath = new ColumnPath();
        accessorColPath.setColumn_family(columnFamily);
        accessorColPath.setColumn(column.getBytes());

        ConsistencyLevel consistency_level = ConsistencyLevel.findByValue(1);

        thriftClient.remove(keyOfAccessor, accessorColPath, 0,
            consistency_level);
    }

    public void dropColumnFamily(String columnFamily) throws Exception {
        thriftClient.system_drop_column_family(columnFamily);
    }

    public void addColumnFamily(String keyspace, String columnFamily)
        throws Exception {
        CfDef c = new CfDef(keyspace, columnFamily);

        thriftClient.system_add_column_family(c);
    }

}

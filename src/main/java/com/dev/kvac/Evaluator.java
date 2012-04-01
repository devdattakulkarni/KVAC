package com.dev.kvac;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dev.kvac.cassandra.CassandraAccessor;
import com.dev.kvac.hbase.HBaseUtil;
import com.dev.kvac.mongodb.MongoDBAccessor;

public final class Evaluator {

    private static final String MONGODB = "mongodb";
    private static final String CASSANDRA = "cassandra";
    private static final String HBASE = "hbase";
    private static final String CURRENT_TIME = "CURRENT_TIME";
    private static final String OP2 = "op2";
    private static final String OP1 = "op1";
    private static final String CONDITION = "condition";
    private static final String VALUE = "value";
    private static final String AND = "and";
    private static final String EQUAL = "equal";
    private static final String IN = "in";

    private static Logger logger = LoggerFactory.getLogger(Evaluator.class);
    private KVStoreInterface kvstore;
    private String storeType;

    public Evaluator(KVStoreInterface kvstore, String storeType) {
        this.kvstore = kvstore;
        this.storeType = storeType;
    }

    public boolean evaluate(String key, Node permissionNode,
        String requestedPermission) {
        boolean result = false;

        NodeList permissionNodeChildren = permissionNode.getChildNodes();
        Node permissionValueNode = null;
        Node conditionNode = null;
        for (int k = 0; k < permissionNodeChildren.getLength(); k++) {
            Node n = permissionNodeChildren.item(k);

            if (n.getNodeName().equals(VALUE)) {
                permissionValueNode = n;
            }
            if (n.getNodeName().equals(CONDITION)) {
                conditionNode = n;
            }
        }

        String specifiedPermission = permissionValueNode.getTextContent()
            .trim();
        if (!specifiedPermission.equalsIgnoreCase(requestedPermission)) {
            return false;
        }

        NodeList conditionNodeChildren = conditionNode.getChildNodes();
        for (int k = 0; k < conditionNodeChildren.getLength(); k++) {
            Node n = conditionNodeChildren.item(k);

            if (n.getNodeName().equals(IN)) {
                result = evaluate_in(key, n);
            }
            if (n.getNodeName().equals(EQUAL)) {
                result = evaluate_equal(key, n);
            }
            if (n.getNodeName().equals(AND)) {
                result = evaluate_and(key, n);
            }
        }

        return result;
    }

    protected boolean evaluate_in(String key, Node inNode) {
        boolean result = false;

        Node lhsNode, rhsNode;

        lhsNode = KVACUtil.getChildNodeByName(inNode, OP1);
        rhsNode = KVACUtil.getChildNodeByName(inNode, OP2);

        if (lhsNode != null && rhsNode != null) {
            String lhs = lhsNode.getTextContent().trim();
            String rhs = rhsNode.getTextContent().trim();

            String lhsResult = evaluate(key, lhs);
            String rhsResult = evaluate(key, rhs);

            if (rhs.equals(CURRENT_TIME) || lhs.equals(CURRENT_TIME)) {

                String specifiedWorkHours = rhs.equals(CURRENT_TIME) ? lhsResult
                    : rhsResult;

                DateFormat dateFormat = new SimpleDateFormat(
                    "yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                int i = compareDates(dateFormat.format(date),
                    specifiedWorkHours);
                if (i == 1)
                    return true;
                else
                    return false;
            }
            return (rhsResult.contains(lhsResult));
        }
        return result;
    }

    protected boolean evaluate_equal(String key, Node andNode) {
        boolean result = false;

        Node lhsNode, rhsNode;

        lhsNode = KVACUtil.getChildNodeByName(andNode, OP1);
        rhsNode = KVACUtil.getChildNodeByName(andNode, OP2);

        if (lhsNode != null && rhsNode != null) {
            String lhs = lhsNode.getTextContent().trim();
            String rhs = rhsNode.getTextContent().trim();

            String lhsResult = evaluate(key, lhs);
            String rhsResult = evaluate(key, rhs);

            return (rhsResult.equals(lhsResult));
        }
        return result;
    }

    protected boolean evaluate_and(String key, Node andNode) {
        boolean result = true;
        NodeList children = andNode.getChildNodes();
        for (int k = 0; k < children.getLength(); k++) {
            Node n = children.item(k);
            if (n.getNodeName().equals(IN)) {
                result = result && evaluate_in(key, n);
            }
            if (n.getNodeName().equals(EQUAL)) {
                result = result && evaluate_equal(key, n);
            }
        }
        return result;
    }

    protected int compareDates(String inputDate, String dateRange) {
        String[] dateEndPoints = dateRange.split("-");
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            Date date = dateFormat.parse(inputDate);
            Date leftEndPoint = dateFormat.parse(dateEndPoints[0]);
            Date rightEndPoint = dateFormat.parse(dateEndPoints[1]);
            if (date.after(leftEndPoint) && date.before(rightEndPoint)) {
                return 1;
            } else {
                return 0;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private String evaluate(String key, String expr) {
        String columnValue = null;

        String keyspace = parseKeySpace(expr);
        String columnFamily = parseColumnFamily(expr);

        if (columnFamily == null) {
            return columnValue;
        }

        String column = parseColumn(expr);
        String rowKey = parseRowKey(key,expr);

        try {
            if (storeType.equalsIgnoreCase(HBASE)) {
                columnValue = HBaseUtil.get(keyspace, rowKey, column, 1);
            }
            if (storeType.equalsIgnoreCase(CASSANDRA)) {

                columnValue = ((CassandraAccessor) kvstore).getCassandraUtil()
                    .get(columnFamily, rowKey, column);
            }
            if (storeType.equalsIgnoreCase(MONGODB)) {
                columnValue = ((MongoDBAccessor) kvstore).getUtil().get(
                    keyspace, columnFamily, rowKey, column);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return columnValue;
    }

    protected static String parseKeySpace(String expression) {
        // input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = "PatientInfoSystem"
        String keySpace = null;
        StringTokenizer tokenizer = new StringTokenizer(expression, "/");

        if (tokenizer.hasMoreTokens()) {
            keySpace = tokenizer.nextToken();
        }
        return keySpace;
    }

    protected static String parseColumnFamily(String expression) {
        // input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = "Patient"
        String columnFamily = null;
        StringTokenizer tokenizer = new StringTokenizer(expression, "/");
        int count = 0;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (count == 1) {
                int index = token.indexOf("(");
                columnFamily = token.substring(0, index > 0 ? index : token
                    .length());
                break;
            } else {
                count++;
            }
        }
        return columnFamily;
    }

    protected static String parseColumn(String expression) {
        // input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = "name"
        String column = null;
        StringTokenizer tokenizer = new StringTokenizer(expression, "/");
        int count = 0;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (count == 2) {
                int index = token.indexOf("(");
                column = token.substring(0, index > 0 ? index : token.length());
                break;
            } else {
                count++;
            }
        }
        return column;
    }

    protected String parseRowKey(String inputKey, String expression) {
        // 1) input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = inputKey
        // 2) input = /PatientInfoSystem/Doctor(key=user.name)/curr_patients
        // output = key constructed from user's username
        // 3) input =
        // /PatientInfoSystem/Doctor(key=/PatientInfoSystem/Patient(key=thisKey)/curr_doctor)/location
        // output = key corresponding to the name of "curr_doctor" for inputKey
        int start = expression.indexOf("(");
        int end = expression.lastIndexOf(")");
        String keyVal = expression.substring(start + 1, end);
        if (keyVal.equals("key=thisKey")) {
            return inputKey;
        } else if (keyVal.equals("key=user.name")) {
            String keyName = this.kvstore.getUser();
            return keyName;
        } else {
            keyVal = keyVal.substring(keyVal.indexOf("/"));
            String keyString = evaluate(inputKey, keyVal);
            return keyString;
        }
    }

    private static ByteBuffer getKey(String keyName) {
        byte[] keyNameArray = keyName.getBytes(Charset.forName("ISO-8859-1"));
        ByteBuffer keyOfAccessor = ByteBuffer.wrap(keyNameArray);
        return keyOfAccessor;
    }
}

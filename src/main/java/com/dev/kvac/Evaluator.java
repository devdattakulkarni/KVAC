package com.dev.kvac;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dev.kvac.cassandra.CassandraAccessor;
import com.dev.kvac.hbase.HBaseUtil;
import com.dev.kvac.mongodb.MongoDBAccessor;

public abstract class Evaluator {

    protected static final String YYYY_MM_DD = "yyyy/MM/dd";
    protected static final String MONGODB = "mongodb";
    protected static final String CASSANDRA = "cassandra";
    protected static final String HBASE = "hbase";
    protected static final String CURRENT_TIME = "CURRENT_TIME";
    protected static final String OP2 = "op2";
    protected static final String OP1 = "op1";
    protected static final String CONDITION = "condition";
    protected static final String VALUE = "value";
    protected static final String AND = "and";
    protected static final String EQUAL = "equal";
    protected static final String IN = "in";
    protected static final String MINUS = "minus";

    private static Logger logger = LoggerFactory.getLogger(Evaluator.class);
    protected KVStoreInterface kvstore;
    protected String storeType;

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

    protected String evaluate_node(String key, Node node) {
        String result = null;
        NodeList lhsNodeChildren = node.getChildNodes();
        if (lhsNodeChildren != null && lhsNodeChildren.getLength() > 0) {
            for (int k = 0; k < lhsNodeChildren.getLength(); k++) {
                Node n = lhsNodeChildren.item(k);
                if (n.getNodeName().equals(MINUS)) {
                    result = evaluate_minus(key, n);
                    return result;
                } else {
                    String lhs = n.getTextContent().trim();
                    if (lhs.equals("")) {
                        continue;
                    }
                    result = evaluate(key, lhs);
                    return result;
                }
            }
        }
        return result;
    }

    protected boolean evaluate_in(String key, Node inNode) {
        boolean result = false;

        Node lhsNode, rhsNode;
        String lhsResult = null;
        String rhsResult = null;

        lhsNode = KVACUtil.getChildNodeByName(inNode, OP1);
        rhsNode = KVACUtil.getChildNodeByName(inNode, OP2);

        if (lhsNode != null) {
            lhsResult = evaluate_node(key, lhsNode);
        }

        if (rhsNode != null) {
            rhsResult = evaluate_node(key, rhsNode);
        }

        if (hasDateNode(lhsNode, false) || hasDateNode(rhsNode, false)) {
            String specifiedWorkHours = hasDateNode(rhsNode, false) ? lhsResult
                : rhsResult;
            DateFormat dateFormat = new SimpleDateFormat(YYYY_MM_DD);
            
            int i = compareDates(lhsResult, rhsResult);
            if (i == 1) {
                return true;
            } else {
                return false;
            }
        }

        result = rhsResult.contains(lhsResult);

        /*
         * if (lhsNode != null && rhsNode != null) { NodeList rhsNodeChildren =
         * rhsNode.getChildNodes(); if (rhsNodeChildren != null &&
         * rhsNodeChildren.getLength() > 0) { for (int k = 0; k <
         * rhsNodeChildren.getLength(); k++) { Node n = rhsNodeChildren.item(k);
         * if (n.getNodeName().equals(MINUS)) { rhsResult = evaluate_minus(key,
         * n); } } } else { String rhs = rhsNode.getTextContent().trim();
         * rhsResult = evaluate(key, rhs); }
         * 
         * result = rhsResult.contains(lhsResult);
         * 
         * String rhs = rhsNode.getTextContent().trim(); rhsResult =
         * evaluate(key, rhs);
         * 
         * return (rhsResult.contains(lhsResult)); }
         */

        return result;
    }

    protected boolean hasDateNode(Node node, boolean result) {

        if (!result) {
            NodeList lhsNodeChildren = node.getChildNodes();
            if (lhsNodeChildren != null && lhsNodeChildren.getLength() > 0) {
                for (int k = 0; k < lhsNodeChildren.getLength(); k++) {
                    Node n = lhsNodeChildren.item(k);
                    if (n.getChildNodes() != null) {
                        result = hasDateNode(n, result);
                    }
                    if (n.getTextContent().trim().equals(CURRENT_TIME)) {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }

    protected String evaluate_minus(String key, Node minusNode) {
        String result = null;
        Node lhsNode = KVACUtil.getChildNodeByName(minusNode, OP1);
        Node rhsNode = KVACUtil.getChildNodeByName(minusNode, OP2);

        String lhsResult = null;
        String rhsResult = null;

        lhsNode = KVACUtil.getChildNodeByName(minusNode, OP1);
        rhsNode = KVACUtil.getChildNodeByName(minusNode, OP2);

        if (lhsNode != null) {
            lhsResult = evaluate_node(key, lhsNode);
        }

        if (rhsNode != null) {
            rhsResult = evaluate_node(key, rhsNode);
        }

        Integer days;
        //DateTime startPoint;

        //DateTimeFormatter fmt = DateTimeFormat.forPattern(YYYY_MM_DD);  
        //DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd");
        //DateTime dateTime = formatter.parseDateTime(dateString);

        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        Date startPoint = null;
        try {
            startPoint = df.parse(lhsResult);
        } catch (Exception exp) {
            try {
                startPoint = df.parse(rhsResult);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        try {
            days = Integer.parseInt(lhsResult);
        } catch (NumberFormatException numberFormatException) {
            days = Integer.parseInt(rhsResult);
        }

        Long time = startPoint.getTime();
        time = time - (days*24*60*60*1000);
        Date endPoint = new Date(time);
        
        DateFormat dateFormat = new SimpleDateFormat(YYYY_MM_DD);
        StringBuffer dateToReturn = new StringBuffer();
        String d1 = dateFormat.format(endPoint);
        String d2 = dateFormat.format(startPoint);
        dateToReturn.append(d1.toString() + "-" + d2.toString());

        result = dateToReturn.toString();

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
            if (n.getNodeName().equals(AND)) {
                result = result && evaluate_and(key, n);
            }
        }
        return result;
    }
    
    protected boolean evaluate_or(String key, Node andNode) {
        boolean result = true;
        NodeList children = andNode.getChildNodes();
        for (int k = 0; k < children.getLength(); k++) {
            Node n = children.item(k);
            if (n.getNodeName().equals(IN)) {
                result = result || evaluate_in(key, n);
            }
            if (n.getNodeName().equals(EQUAL)) {
                result = result || evaluate_equal(key, n);
            }
        }
        return result;
    }
    
    protected int compareDates(String lhs, String rhs) {
        String inputDate = lhs;
        String dateRange = rhs;
        if (lhs.contains("-")) {
            dateRange = lhs;
            inputDate = rhs;
        }
       
        String[] dateEndPoints = dateRange.split("-");
        DateFormat dateFormat = new SimpleDateFormat(YYYY_MM_DD);
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

    public String evaluate(String key, String expr) {

        if (expr.equals(CURRENT_TIME)) {
            DateFormat dateFormat = new SimpleDateFormat(YYYY_MM_DD);
            Date date = new Date();
            return dateFormat.format(date);
        } else if (expr.toUpperCase().contains("DAYS")) {
            int i = expr.indexOf("(");
            int j = expr.indexOf(")");
            return expr.substring(i + 1, j);
        }

        String columnValue = null;

        String keyspace = parseKeySpace(expr);
        String columnFamily = parseColumnFamily(expr);

        if (columnFamily == null) {
            return columnValue;
        }

        String[] columnNameValue = parseColumn(expr);
        String column = columnNameValue[0];
        String parameterizedColValue = columnNameValue[1];
        String rowKey = parseRowKey(key, expr);

        try {
            
            columnValue = getAttributeValue();
            
            if (storeType.equalsIgnoreCase(HBASE)) {
                columnValue = HBaseUtil.get(keyspace, rowKey, column, 1);
            }
            if (storeType.equalsIgnoreCase(CASSANDRA)) {
                byte [] val = (byte[])((CassandraAccessor) kvstore).getCassandraUtil()
                    .get(columnFamily, rowKey, column);
                
                BufferedInputStream buffer = new BufferedInputStream( new ByteArrayInputStream(val) );
                ObjectInput input = new ObjectInputStream ( buffer );
                
                columnValue = (String)input.readObject();

                if (parameterizedColValue != null) {
                    String actualColValue = kvstore
                        .getRuntimeParameterValues(parameterizedColValue);
                    if (columnValue.contains(actualColValue)) {
                        return actualColValue;
                    } else {
                        return null;
                    }
                }
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
    
    public String getAttributeValue() {
        return null;
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

    protected static String[] parseColumn(String expression) {
        // input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = "name"
        // input =
        // /PatientInfoSystem/Doctor(key=/PatientInfoSystem/Patient(key=thisKey)/curr_doctor)/location
        // output = "location"
        // input =
        // /SocialShare/Person(key=thisKey)/message(value=$c)
        // output = message

        String[] colNameValueArr = new String[2];
        String column = null;
        String value = null;

        int lastSlashIndex = expression.lastIndexOf("/");
        column = expression.substring(lastSlashIndex + 1);

        int firstParenIndex = column.indexOf("(");
        if (firstParenIndex >= 0) {
            value = column.substring(firstParenIndex + 1, column
                .lastIndexOf(""));
            value = value.substring(value.indexOf("=") + 1, value.length() - 1);
            column = column.substring(0, firstParenIndex);
        }
        colNameValueArr[0] = column;
        colNameValueArr[1] = value;

        return colNameValueArr;
    }

    protected String parseRowKey(String inputKey, String expression) {
        // 1) input = /PatientInfoSystem/Patient(key=thisKey)/name
        // output = inputKey
        // 2) input = /PatientInfoSystem/Doctor(key=user.name)/curr_patients
        // output = key constructed from user's username
        // 3) input =
        // /PatientInfoSystem/Doctor(key=/PatientInfoSystem/Patient(key=thisKey)/curr_doctor)/location
        // output = key corresponding to the name of "curr_doctor" for inputKey
        // 4) input =
        // /SocialShare/Message(key=/SocialShare/Person(key=thisKey)/messageId(value=$c))/message_time_stamp
        // output = key corresponding to the messageId that matches the runtime
        // parameter c for inputKey.
        int start = expression.indexOf("(");
        int end = expression.indexOf(")");
        String keyVal = expression.substring(start + 1, end);

        // Pattern thisKeyPattern = Pattern.compile("key=thisKey");
        // Matcher thisKeyMatcher = thisKeyPattern.matcher(keyVal);

        // Pattern userNamePattern = Pattern.compile("key=user.name");
        // Matcher userNameMatcher = userNamePattern.matcher(keyVal);

        if (keyVal.equals("key=thisKey")) {
            return inputKey;
        } else if (keyVal.equals("key=user.name")) {
            String keyName = this.kvstore.getUser();
            return keyName;
        } else {
            int i = expression.indexOf("(");
            int j = expression.lastIndexOf(")");
            keyVal = expression.substring(i + 1, j);
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

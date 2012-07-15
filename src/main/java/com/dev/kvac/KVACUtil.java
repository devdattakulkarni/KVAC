package com.dev.kvac;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class KVACUtil {

    private static final String PERMISSION = "permission";
    private static final String RESOURCE = "resource";
    private static final String POLICY_NODE = "policy";

    // Returns a Map of resourceName, Permission Node. 
    // Currently this does not support multiple permission nodes for a given resource.
    public static Map<Node, Node> readPolicyFile(String policyFilePath)
        throws Exception {
        Map<Node, Node> resourceNamePermissionNodeMap = new LinkedHashMap<Node, Node>();
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(policyFilePath);
            NodeList policyList = dom.getElementsByTagName(POLICY_NODE);
            Node resource = null;
            Node permissionNode = null;
            for (int i = 0; i < policyList.getLength(); i++) {
                Node policyNode = policyList.item(i);
                NodeList children = policyNode.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    if (child.getNodeName().equals(RESOURCE)) {
                        String res = child.getTextContent();
                        res = res.replaceAll(" ", "").replaceAll(
                            "\n", "");
                        //System.out.println("(KVACUtil:readPolicyFile) Resource:" + res);
                        resource = child;
                    }
                    if (child.getNodeName().equals(PERMISSION)) {
                        permissionNode = child;
                    }
                    if (resource != null && permissionNode != null) {
                        resourceNamePermissionNodeMap.put(resource, permissionNode);
                        resource = null;
                        permissionNode = null;
                    }
                }
            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (SAXException se) {
            se.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return resourceNamePermissionNodeMap;
    }

    public static Node getChildNodeByName(Node node, String childName) {
        NodeList children = node.getChildNodes();
        for (int j = 0; j < children.getLength(); j++) {
            Node child = children.item(j);
            if (child.getNodeName().equals(childName)) {
                return child;
            }
        }
        return null;
    }
    
    public static Object[] findPermissionNodeForResource(Map<Node,Node> resourcePolicyMap, String resource) {
        Object[] resTypeAndPermission = new Object[2];

        for (Node resourceNode : resourcePolicyMap.keySet()) {
            String specifiedResource = resourceNode.getTextContent();
            specifiedResource = specifiedResource.replaceAll(" ", "")
                .replaceAll("\n", "");
            if (specifiedResource.equalsIgnoreCase(resource)) {
                Node permissionNode = resourcePolicyMap.get(resourceNode);
                resTypeAndPermission[1] = permissionNode;

                String resType = resourceNode.getAttributes().getNamedItem(
                    "type").getNodeValue();
                resTypeAndPermission[0] = resType;
            }
        }
        return resTypeAndPermission;
    }
    
    public static String getStringRepresentation(ByteBuffer key) {
        Charset charset = Charset.forName("ISO-8859-1");
        CharsetDecoder decoder = charset.newDecoder();
        String keyName = null;
        ByteBuffer keyNameByteBuffer = key.duplicate();
        CharBuffer keyNameCharBuffer;
        try {
            keyNameCharBuffer = decoder.decode(keyNameByteBuffer);
            keyName = keyNameCharBuffer.toString();
        } catch (CharacterCodingException e1) {
            e1.printStackTrace();
        }
        return keyName;
    }    

}
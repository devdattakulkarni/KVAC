package com.dev.kvac;

import java.io.IOException;
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
    public static Map<String, Node> readPolicyFile(String policyFilePath)
        throws Exception {
        Map<String, Node> resourceNamePermissionNodeMap = new LinkedHashMap<String, Node>();
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(policyFilePath);
            NodeList policyList = dom.getElementsByTagName(POLICY_NODE);
            String resource = null;
            Node permissionNode = null;
            for (int i = 0; i < policyList.getLength(); i++) {
                Node policyNode = policyList.item(i);
                NodeList children = policyNode.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    if (child.getNodeName().equals(RESOURCE)) {
                        resource = child.getTextContent();
                        resource = resource.replaceAll(" ", "").replaceAll(
                            "\n", "");
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

}
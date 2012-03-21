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

    public static Map<String, Node> readPolicyFile(String policyFilePath)
        throws Exception {
        Map<String, Node> resourcePolicyMap = new LinkedHashMap<String, Node>();
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(policyFilePath);
            NodeList policyList = dom.getElementsByTagName("policy");
            String resource = null;
            Node policy = null;
            for (int i = 0; i < policyList.getLength(); i++) {
                Node policyNode = policyList.item(i);
                NodeList children = policyNode.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    if (child.getNodeName().equals("access")) {
                        resource = child.getTextContent();
                        resource = resource.replaceAll(" ", "").replaceAll(
                            "\n", "");
                    }
                    if (child.getNodeName().equals("where")) {
                        policy = child;
                    }
                    if (resource != null && policy != null) {
                        resourcePolicyMap.put(resource, policy);
                        resource = null;
                        policy = null;
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

        return resourcePolicyMap;
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
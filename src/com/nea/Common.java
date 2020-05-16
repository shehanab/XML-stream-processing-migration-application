
package com.nea;

import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class Common {

    public void getLog4j(Logger logger) throws Exception {
        Properties props = new Properties();
        props.load((MigrationMain.class).getResourceAsStream("log4j.properties"));
        logger.debug("Logger configuration via log4j.properties via has loaded successfully using the path ["
                + getClass().getResourceAsStream("/log4j.properties") + "]");
        PropertyConfigurator.configure(props);
    }

    public HashMap<String, String> getConfiguration(
            String configFileNameOnlyWithExt, String classRootTagName) {

        HashMap<String, String> configurationProperties = null;
        DocumentBuilderFactory dbFactory = null;
        DocumentBuilder dBuilder = null;
        Document doc = null;

        try {
            dbFactory = DocumentBuilderFactory.newInstance();
            dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(configFileNameOnlyWithExt);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName(classRootTagName);

            for (int i = 0; i < nList.getLength(); i++) {
                Node nNode = nList.item(i);
                configurationProperties = new HashMap<String, String>();
                for (int j = 0; j < nNode.getChildNodes().getLength(); j++) {
                    if (nNode.getChildNodes().item(j).getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) nNode.getChildNodes().item(j);
                        configurationProperties.put(eElement.getTagName(), eElement.getTextContent());
                    }
                }
            }
            return configurationProperties;

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (null != doc) {
                doc = null;
            }
            if (null != dBuilder) {
                dBuilder = null;
            }
            if (null != dbFactory) {
                dbFactory = null;
            }
        }
        return null;
    }

    public HashMap<String, String> readConfig() {
        return this.getConfiguration((MigrationMain.class).getResource("Config.xml").toString(),
                "Config");
    }

    public ArrayList<HashMap<String, String>> readXML(String MetadataInputFolder, Logger logger) {
        ArrayList<HashMap<String, String>> results = new ArrayList<HashMap<String, String>>();
        ArrayList<File> fileList = new ArrayList<File>();
        readXMLsFrmSubfolders(MetadataInputFolder, fileList);

        for (File file : fileList) {
            logger.info("Metadata file: " + file.getPath());
            ArrayList<HashMap<String, String>> metadata = getMetadata(file.getPath());
            results.addAll(metadata);
        }

        return results;
    }

    public void readXMLsFrmSubfolders(String directoryName, ArrayList<File> fileList) {
        File directory = new File(directoryName);

        // Get all files from a directory.
        File[] fList = directory.listFiles();

        if (fList != null) {
            Arrays.sort(fList, NameFileComparator.NAME_COMPARATOR);
            for (File file : fList) {
                if (file.isFile() && "metadata.xml".equals(file.getName().toLowerCase())) {
                    fileList.add(file);
                } else if (file.isDirectory()) {
                    readXMLsFrmSubfolders(file.getAbsolutePath(), fileList);
                }
            }
        }
    }

    public ArrayList<HashMap<String, String>> getMetadata(String filePath) {
        File fXmlFile = new File(filePath);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;
        Document doc;
        HashMap<String, String> metadata;
        ArrayList<HashMap<String, String>> rs = new ArrayList<HashMap<String, String>>();

        try {
            dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("DocumentDetails");

            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element eElement = (Element) nNode;
                    metadata = new HashMap<String, String>();

                    metadata.put("ObjectID", eElement.getElementsByTagName("ObjectID").item(0).getTextContent());
                    metadata.put("Name", eElement.getElementsByTagName("Name").item(0).getTextContent());
                    metadata.put("Type", eElement.getElementsByTagName("Type").item(0).getTextContent());
                    metadata.put("Subject", eElement.getElementsByTagName("Subject").item(0).getTextContent());
                    metadata.put("Title", eElement.getElementsByTagName("Title").item(0).getTextContent());
                    metadata.put("CreatorName", eElement.getElementsByTagName("CreatorName").item(0).getTextContent());
                    metadata.put("CreatedDate", eElement.getElementsByTagName("CreatedDate").item(0).getTextContent());
                    metadata.put("Format", eElement.getElementsByTagName("Format").item(0).getTextContent());
                    metadata.put("OwnerName", eElement.getElementsByTagName("OwnerName").item(0).getTextContent());
                    metadata.put("appl_no", eElement.getElementsByTagName("appl_no").item(0).getTextContent());
                    metadata.put("lic_no", eElement.getElementsByTagName("lic_no").item(0).getTextContent());
                    metadata.put("r_full_content_size", eElement.getElementsByTagName("r_full_content_size").item(0).getTextContent());
                    metadata.put("folderpath", eElement.getElementsByTagName("folderpath").item(0).getTextContent());
                    metadata.put("filechecksum", eElement.getElementsByTagName("filechecksum").item(0).getTextContent());

                    rs.add(metadata);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }

}

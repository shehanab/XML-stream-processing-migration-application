package com.nea;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.Action;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisConnectionException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisUnauthorizedException;
import org.apache.chemistry.opencmis.commons.impl.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AlfrescoCommon {

    private static final Logger logger = Logger.getLogger(AlfrescoCommon.class);
    private static Map<String, Session> connections = new ConcurrentHashMap<String, Session>();

    public Session getSession(String connectionName, String username, String pwd, String url, Logger logger) {
        Session session = connections.get(connectionName);
        if (session == null) {
            logger.info("Not connected, creating new connection to Alfresco with the connection id (" + connectionName + ")");

            // No connection to Alfresco available, create a new one
            SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(SessionParameter.USER, username);
            parameters.put(SessionParameter.PASSWORD, pwd);
            parameters.put(SessionParameter.ATOMPUB_URL, url);
            parameters.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
            parameters.put(SessionParameter.COMPRESSION, "true");
            parameters.put(SessionParameter.CACHE_TTL_OBJECTS, "0");

            List<Repository> repositories = sessionFactory.getRepositories(parameters);
            Repository alfrescoRepository = null;
            if (repositories != null && repositories.size() > 0) {
                logger.info("Found (" + repositories.size() + ") Alfresco repositories");
                alfrescoRepository = repositories.get(0);
                logger.info("Info about the first Alfresco repo [ID=" + alfrescoRepository.getId() +
                        "][name=" + alfrescoRepository.getName() +
                        "][CMIS ver supported=" + alfrescoRepository.getCmisVersionSupported() + "]");
            } else {
                throw new CmisConnectionException("Could not connect to the Alfresco Server, no repository found!");
            }

            // Create a new session with the Alfresco repository
            session = alfrescoRepository.createSession();

            // Save connection for reuse
            connections.put(connectionName, session);
        } else {
            logger.info("Already connected to Alfresco with the connection id (" + connectionName + ")");
        }

        return session;
    }

    public String uploadToAlfresco(HashMap<String, String> config, String folderPath, File file, DatabaseOperations databaseOperations,
                                   String documentName, HashMap<String, String> curMetadata, String curDate) {

        Session session = getSession("OneILS",
                config.get("ALFRESCOUSERNAME"), config.get("ALFRESCOPASSWORD"), config.get("ALFRESCOURL"), logger);

        Document newDoc = createDocumentFromFileWithCustomType(session, documentName, folderPath,
                file, config.get("ALFRESCOROOTFOLDER"), config.get("ErrorFolder"),
                curMetadata, curDate, databaseOperations);

        return newDoc != null ? newDoc.getContentUrl() : null;
    }

    public Document createDocumentFromFileWithCustomType(Session session, String documentName, String folderPath,
                                                         File file, String alfrescoRoot, String errFolder,
                                                         HashMap<String, String> curMetadata, String curDate, DatabaseOperations databaseOperations) {
        createFolder(session, alfrescoRoot + folderPath);
        Document document = (Document) getObject(session, documentName, alfrescoRoot + folderPath);
        // Check if document already exist, if so delete and create it
        if (document != null) {
            //Document already exists delete and re-upload
            logger.info("Document already exist: " + getDocumentPath(document, logger));
            document.delete(true);
            databaseOperations.deleteRecord(curMetadata.get("ObjectID"));
            logger.info("Deleted document " + documentName);
            logger.info("Uploading modified document " + documentName);
        }

        return createNewDocument(session, documentName, folderPath, alfrescoRoot,
                errFolder, curMetadata, curDate, file);
    }

    public void createFolder(Session session, String folderPath) {
        Folder parentFolder = session.getRootFolder();

        // Make sure the user is allowed to create a folder under the root folder
        if (parentFolder.getAllowableActions().getAllowableActions().contains(Action.CAN_CREATE_FOLDER) == false) {
            throw new CmisUnauthorizedException("Current user does not have permission to create a sub-folder in " +
                    parentFolder.getPath());
        }

        String[] folders = folderPath.split("/");
        String currentPath = "/";
        Folder currentFolder = parentFolder;

        for (String folder : folders) {
            Folder newFolder = (Folder) getObject(session, currentPath + folder);
            if (newFolder == null) {
                Map<String, Object> newFolderProps = new HashMap<String, Object>();
                newFolderProps.put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder");
                newFolderProps.put(PropertyIds.NAME, folder);

                newFolder = currentFolder.createFolder(newFolderProps);

                logger.info("Created new folder: " + newFolder.getPath() +
                        " [creator=" + newFolder.getCreatedBy() + "][created=" +
                        date2String(newFolder.getCreationDate().getTime()) + "]");

            } else {
                logger.info("Folder already exist: " + newFolder.getPath());
            }

            currentPath = newFolder.getPath() + "/";
            currentFolder = newFolder;
        }

    }

    private Document createNewDocument(Session session, String documentName, String folderPath, String alfrescoRoot,
                                       String errFolder, HashMap<String, String> curMetadata,
                                       String curDate, File file) {
        Document document;// Setup document metadata
        Map<String, Object> newDocumentProps = new HashMap<String, Object>();
        newDocumentProps.put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
        newDocumentProps.put(PropertyIds.NAME, documentName);

        InputStream is = null;
        try {
            // Setup document content
            is = new FileInputStream(file);
            String mimetype = "charset=UTF-8";
            ContentStream contentStream = session.getObjectFactory()
                    .createContentStream(documentName, file.length(), mimetype, is);

            // Create versioned document object
            document = ((Folder) session.getObjectByPath("/" + alfrescoRoot + folderPath))
                    .createDocument(newDocumentProps, contentStream, VersioningState.MAJOR);

            logger.info("Created new document: " + getDocumentPath(document, logger) +
                    " [version=" + document.getVersionLabel() + "][creator=" + document.getCreatedBy() +
                    "][created=" + date2String(document.getCreationDate().getTime()) + "]");

            // Close the stream to handle any IO Exception
            is.close();
        } catch (IOException ioe) {
            writeTxt(errFolder + "error_" + curDate + ".txt",
                    "Object ID: " + curMetadata.get("ObjectID") + "	name: " + curMetadata.get("Name") +
                            "	folderpath: " + curMetadata.get("folderpath"));
            logger.info(ioe.getMessage());
            ioe.printStackTrace();
            document = null;
        } finally {
            IOUtils.closeQuietly(is);
        }
        return document;
    }

    public void writeTxt(String f, String data) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(f, true));
            PrintWriter out = new PrintWriter(writer);
            out.println(data);
            out.close();
        } catch (Exception E) {
        }
    }

    private CmisObject getObject(Session session, String objectName) {
        CmisObject object = null;

        try {
            object = session.getObjectByPath(objectName);
        } catch (CmisObjectNotFoundException nfe0) {
            // Nothing to do, object does not exist
        }

        return object;
    }

    private CmisObject getObject(Session session, String fileName, String folderPath) {
        CmisObject object = null;

        try {
            String path2Object = session.getRootFolder().getPath();
            if (!path2Object.endsWith("/")) {
                path2Object += "/";
            }

            path2Object += folderPath + "/";
            path2Object += fileName;
            object = session.getObjectByPath(path2Object);
        } catch (CmisObjectNotFoundException nfe0) {
            // Nothing to do, object does not exist
        }

        return object;
    }

    private String getDocumentPath(Document document, Logger logger) {
        String path2Doc = getParentFolderPath(document, logger);
        if (!path2Doc.endsWith("/")) {
            path2Doc += "/";
        }
        path2Doc += document.getName();
        return path2Doc;
    }

    private String getParentFolderPath(Document document, Logger logger) {
        Folder parentFolder = getDocumentParentFolder(document, logger);
        return parentFolder == null ? "Un-filed" : parentFolder.getPath();
    }

    private Folder getDocumentParentFolder(Document document, Logger logger) {
        // Get all the parent folders (could be more than one if multi-filed)
        List<Folder> parentFolders = document.getParents();

        // Grab the first parent folder
        if (parentFolders.size() > 0) {
            if (parentFolders.size() > 1) {
                logger.info("The " + document.getName() + " has more than one parent folder, it is multi-filed");
            }

            return parentFolders.get(0);
        } else {
            logger.info("Document " + document.getName() + " is un-filed and does not have a parent folder");
            return null;
        }
    }

    private String date2String(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").format(date);
    }

}

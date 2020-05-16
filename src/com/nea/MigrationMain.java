package com.nea;


import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class MigrationMain {

    private static final Logger logger = Logger.getLogger(MigrationMain.class);

    static {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hhmmss");
        System.setProperty("current.date", dateFormat.format(new Date()));
    }

    public static void main(String[] args) {

        DatabaseOperations databaseOperations = new DatabaseOperations();
        ChecksumGenerator checksumGenerator = new ChecksumGenerator();
        AlfrescoCommon alfrescoCommon = new AlfrescoCommon();
        Common common = new Common();

        try {

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-dd-MM HHmmss");
            String curDate = formatter.format(new Date());
            common.getLog4j(logger);
            logger.info("Start processing...");

            HashMap<String, String> config = common.readConfig();
            databaseOperations.setDBConnection(config);
            logger.info("Read configuration file completed!");

            ArrayList<HashMap<String, String>> metadataLs = common.readXML(config.get("MetadataInputFolder"), logger);
            logger.info("Read XML files completed!");
            logger.info("Total " + metadataLs.size() + " documents to be uploaded!");

            //loop every xml file
            for (int i = 0; i < metadataLs.size(); i++) {

                HashMap<String, String> curMetadata = metadataLs.get(i);
                File file = new File(config.get("DocFolder") + curMetadata.get("folderpath") + "/" + curMetadata.get("ObjectID") + "_" + curMetadata.get("Name"));
                //loop each document
                String docUrl = alfrescoCommon.uploadToAlfresco(config, curMetadata.get("folderpath"), file, databaseOperations,
                        curMetadata.get("ObjectID") + "_" + curMetadata.get("Name"), curMetadata, curDate);

                if (docUrl != null) {
                    //Generate checksum for the current file
                    String currentFileChecksum = checksumGenerator.generateChecksum(file);

                    //update db
                    databaseOperations.insertRecord(curMetadata, docUrl, currentFileChecksum);
                    logger.info(i + " Uploaded Object ID: " + curMetadata.get("ObjectID")
                            + " name: " + curMetadata.get("Name")
                            + " folderpath: " + curMetadata.get("folderpath"));
                } else {
                    logger.info(i + " Object ID: " + curMetadata.get("ObjectID")
                            + " name: " + curMetadata.get("Name")
                            + " folderpath: " + curMetadata.get("folderpath") + " exists and SKIP uploading");
                }
            }

            databaseOperations.closeDBConnection();
            logger.info("End processing...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

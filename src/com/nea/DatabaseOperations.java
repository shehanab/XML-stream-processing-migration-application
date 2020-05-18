package com.nea;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

public class DatabaseOperations {

    private static final Logger logger = Logger.getLogger(DatabaseOperations.class);

    private static final String SQL_INSERT_PARAMS = "INSERT INTO TBL_MIGRATION (ObjectID,Name,Type,Subject,Title,CreatorName,CreatedDate,Format,OwnerName,appl_no,lic_no,r_full_content_size,folderpath,filechecksum,targetFileChecksum,ContentURL) ";
    private static final String SQL_INSERT_VALUES = "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String SQL_DELETE_RECORD = "DELETE FROM TBL_MIGRATION WHERE ObjectID = ?";
    private Connection con;

    public void setDBConnection(HashMap<String, String> config) throws Exception {
        DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());

        if (con == null || con.isClosed()) {
            con = DriverManager.getConnection(config.get("DBConnection"));
        }
    }

    public void closeDBConnection() {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            logger.error("DB ERROR OCCURRED ", e);
            e.printStackTrace();
        }
    }

    public void insertRecord(HashMap<String, String> curMetadata, String docUrl, String currentFileChecksum) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(SQL_INSERT_PARAMS);
        sqlBuilder.append(SQL_INSERT_VALUES);
        PreparedStatement ps;
        try {
            ps = con.prepareStatement(sqlBuilder.toString());
            ps.setString(1, curMetadata.get("ObjectID"));
            ps.setString(2, curMetadata.get("Name"));
            ps.setString(3, curMetadata.get("Type"));
            ps.setString(4, curMetadata.get("Subject"));
            ps.setString(5, curMetadata.get("Title"));
            ps.setString(6, curMetadata.get("CreatorName"));
            ps.setString(7, curMetadata.get("CreatedDate"));
            ps.setString(8, curMetadata.get("Format"));
            ps.setString(9, curMetadata.get("OwnerName"));
            ps.setString(10, curMetadata.get("appl_no"));
            ps.setString(11, curMetadata.get("lic_no"));
            ps.setString(12, curMetadata.get("r_full_content_size"));
            ps.setString(13, curMetadata.get("folderpath"));
            ps.setString(14, curMetadata.get("filechecksum"));
            ps.setString(15, currentFileChecksum);
            ps.setString(16, docUrl);

            ps.executeUpdate();

            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("DB ERROR OCCURRED ", e);
            e.printStackTrace();
        }
    }

    public void deleteRecord(String ObjectID) {
        PreparedStatement ps;
        try {
            ps = con.prepareStatement(SQL_DELETE_RECORD);
            ps.setString(1, ObjectID);
            ps.executeUpdate();
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("DB ERROR OCCURRED ", e);
            e.printStackTrace();
        }
    }

}

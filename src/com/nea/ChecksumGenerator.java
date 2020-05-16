package com.nea;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.comparator.NameFileComparator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.commons.io.IOUtils.toByteArray;

public class ChecksumGenerator {

    /*public static void main(String[] args) {
        // TODO Auto-generated method stub
        ArrayList<File> fileList = new ArrayList<File>();
        StringBuilder sb = new StringBuilder();
        checksum main = new checksum();
        main.readFilesFrmSubfolders(args[0], fileList);

        for (int i = 0; i < fileList.size(); i++) {
            writeChecksum(args[1], fileList.get(i), sb, main);
        }
    }*/


    public String generateChecksum(File file) {
        try (InputStream is = Files.newInputStream(Paths.get(file.getPath()))) {
            return DigestUtils.md5Hex(toByteArray(is));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }


    private void writeChecksum(String arg, File file, StringBuilder sb, ChecksumGenerator main) {

        String md5 = generateChecksum(file);
        sb.append(file.getPath()).append(",").append(md5);
        try {
            main.writeTxt(arg, sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        sb.setLength(0);

    }

    public void readFilesFrmSubfolders(String directoryName, ArrayList<File> fileList) {
        File directory = new File(directoryName);

        // Get all files from a directory.
        File[] fList = directory.listFiles();

        if (fList != null) {
            Arrays.sort(fList, NameFileComparator.NAME_COMPARATOR);

            for (File file : fList) {
                if (file.isFile()) {
                    fileList.add(file);
                } else if (file.isDirectory()) {
                    readFilesFrmSubfolders(file.getAbsolutePath(), fileList);
                }
            }
        }
    }

    public void writeTxt(String f, String data) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(f, true));
        PrintWriter out = new PrintWriter(writer);
        out.println(data);
        out.close();
    }

}

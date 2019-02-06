package com.parallelmachines.reflex.common.util;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileOps {
    public static String readFileIntoString(String filename) throws IOException, FileNotFoundException {
        File f = new File(filename);
        if (!f.exists()) {
            throw new FileNotFoundException("Could not find file: [" + filename + "]");
        }
        if (!f.isFile()) {
            throw new FileNotFoundException("File [" + filename + "] is not a regular file.");
        }
        String s = FileUtils.readFileToString(f, "UTF-8");
        return s;
    }
}
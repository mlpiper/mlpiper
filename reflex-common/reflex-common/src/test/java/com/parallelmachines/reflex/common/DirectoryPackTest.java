package com.parallelmachines.reflex.common;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import java.nio.file.*;
import java.io.*;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.MessageFormat;
import java.util.Arrays;


/**
 * Unit test for simple App.
 */
public class DirectoryPackTest
    extends TestCase
{
    private static final Logger logger = Logger.getLogger(DirectoryPackTest.class);

    private String tmpDir = "/tmp";
    private String files[];
    /**
     +     * Create the test case
     +     *
     +     * @param testName name of the test case
     +     */
    public DirectoryPackTest(String testName )
    {

        super( testName );
        files = new String[3];
        files[0] = "test/test.txt";
        files[1] = "test1.txt";
        files[2] = "test2.txt";
    }

    /**
    * @return the suite of tests being tested
    */
    public static Test suite()
    {
        BasicConfigurator.configure();
        return new TestSuite( DirectoryPackTest.class );
    }

    /**
    * Rigourous Test :-)
    */
    public void testApp()
    {
        assertTrue( true );
    }

    private void generateTestDir(String dirName, String[] files) throws IOException {
        generateTestDir(dirName, files, "Hello World");
    }


    private void generateTestDir(String dirName, String[] files, String content) throws IOException {

        for (int i = 0; i < files.length; i++) {
            File targetFile = new File(Paths.get(dirName, files[i]).toString());
            File parent = targetFile.getParentFile();
            if (!parent.exists() && !parent.mkdirs()) {
                throw new IllegalStateException("Couldn't create dir: " + parent);
            } else {
                targetFile.createNewFile();
                FileWriter fw = new FileWriter(targetFile);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content);
                bw.close();
                fw.close();
            }
        }
    }

    private static void verifyDirsAreEqual(Path one, Path other) throws Exception {
        Files.walkFileTree(one, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attrs)
                    throws IOException {
                FileVisitResult result = super.visitFile(file, attrs);

                // get the relative file name from path "one"
                Path relativize = one.relativize(file);
                // construct the path for the counterpart file in "other"
                Path fileInOther = other.resolve(relativize);
                logger.debug(MessageFormat.format("=== comparing: {0} to {1}", file, fileInOther));

                byte[] otherBytes = Files.readAllBytes(fileInOther);
                byte[] thisBytes = Files.readAllBytes(file);
                if (!Arrays.equals(otherBytes, thisBytes)) {
                    throw new IOException(file + " is not equal to " + fileInOther);
                }
                return result;
            }
        });
    }

    public void testVerifyDirsAreEqual() throws Exception {
        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String srcPath1 = Paths.get(tmpDir, testPrefix).toString();
        String srcPath2 = Paths.get(tmpDir, testPrefix + "-dest").toString();

        generateTestDir(srcPath1, files);
        generateTestDir(srcPath2, files);

        verifyDirsAreEqual(Paths.get(srcPath1), Paths.get(srcPath1));
        FileUtils.deleteDirectory(new File(srcPath1));
        FileUtils.deleteDirectory(new File(srcPath2));

    }

    public void testVerifyDirsAreEqualBad() throws Exception {
        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String srcPath1 = Paths.get(tmpDir, testPrefix).toString();
        String srcPath2 = Paths.get(tmpDir, testPrefix + "-dest").toString();

        generateTestDir(srcPath1, files);
        String files2[] = files.clone();
        files2[0] += "aaaa";
        generateTestDir(srcPath2, files2);

        try {
            verifyDirsAreEqual(Paths.get(srcPath1), Paths.get(srcPath2));
            fail("The code above should throw an exception");
        } catch(Exception e){
            // this is ok
        }

        FileUtils.deleteDirectory(new File(srcPath1));
        FileUtils.deleteDirectory(new File(srcPath2));

    }

    public void testVerifyDirsAreEqualBad2() throws Exception {
        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String srcPath1 = Paths.get(tmpDir, testPrefix).toString();
        String srcPath2 = Paths.get(tmpDir, testPrefix + "-dest").toString();

        generateTestDir(srcPath1, files, "ABCDEFG");
        generateTestDir(srcPath2, files, "1122334455");

        try {
            verifyDirsAreEqual(Paths.get(srcPath1), Paths.get(srcPath2));
            fail("The code above should throw an exception");
        } catch(Exception e){
            // this is ok
        }

        FileUtils.deleteDirectory(new File(srcPath1));
        FileUtils.deleteDirectory(new File(srcPath2));

    }
    public void testPackUnPack() throws Exception {
        DirectoryPack dP = new DirectoryPack(tmpDir);

        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String sourcePath = Paths.get(tmpDir, testPrefix).toString();
        String destPath = Paths.get(tmpDir, testPrefix + "-dest").toString();
        String extractPath = Paths.get(tmpDir, testPrefix + "-extract").toString();

        String files2[] = files.clone();
        int len = 100;
        for (int i=0 ; i < len ; i++) {
            files2[0] += "s";
        }
        generateTestDir(sourcePath, files2);

        new File(destPath).mkdirs();
        new File(extractPath).mkdirs();

        // Do pack and unpack
        byte[] stream = dP.pack(sourcePath);
        dP.unPack(stream, extractPath);

        // Check if contents are equal
        verifyDirsAreEqual(Paths.get(sourcePath), Paths.get(extractPath, testPrefix));

        FileUtils.deleteDirectory(new File(sourcePath));
        FileUtils.deleteDirectory(new File(destPath));
        FileUtils.deleteDirectory(new File(extractPath));
    }

    public void testPackUnPackLongName() throws Exception {
        DirectoryPack dP = new DirectoryPack(tmpDir);

        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String sourcePath = Paths.get(tmpDir, testPrefix).toString();
        String destPath = Paths.get(tmpDir, testPrefix + "-dest").toString();
        String extractPath = Paths.get(tmpDir, testPrefix + "-extract").toString();

        generateTestDir(sourcePath, files);

        new File(destPath).mkdirs();
        new File(extractPath).mkdirs();

        // Do pack and unpack
        byte[] stream = dP.pack(sourcePath);
        dP.unPack(stream, extractPath);

        // Check if contents are equal
        verifyDirsAreEqual(Paths.get(sourcePath), Paths.get(extractPath, testPrefix));

        FileUtils.deleteDirectory(new File(sourcePath));
        FileUtils.deleteDirectory(new File(destPath));
        FileUtils.deleteDirectory(new File(extractPath));
    }



    public void testGzipUnzip() throws  Exception {
        BasicConfigurator.configure();

        long currentTime = System.currentTimeMillis();
        String testPrefix = "test-" + currentTime;
        String sourcePath = Paths.get(tmpDir, testPrefix).toString();
        String destPath = Paths.get(tmpDir, testPrefix + "-dest.tar.gz").toString();
        String extractPath = Paths.get(tmpDir, testPrefix + "-extract").toString();

        generateTestDir(sourcePath, files);

        DirectoryPack dp = new DirectoryPack(tmpDir);

        new File(extractPath).mkdirs();

        dp.gZipIt(sourcePath, destPath);
        dp.unGZip(destPath, extractPath);

        verifyDirsAreEqual(Paths.get(sourcePath), Paths.get(extractPath, testPrefix));

        FileUtils.deleteDirectory(new File(sourcePath));
        FileUtils.forceDelete(new File(destPath));
        FileUtils.deleteDirectory(new File(extractPath));
    }
}

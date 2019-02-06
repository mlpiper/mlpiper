package com.parallelmachines.reflex.common;

import com.parallelmachines.reflex.common.exceptions.ReflexCommonException;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.BeforeClass;

import java.io.File;
import java.util.UUID;

public class ExtractArchivesTest {
	private static String workingDir;
	private static String testFileDir;

	@BeforeClass
	public static void setUp() {
		workingDir = System.getProperty("user.dir");
		testFileDir = workingDir + "/test-file/extract-archives";
	}

	@Test
	public void testTarSingleFile() throws Exception {
		String testFilePath = testFileDir + "/extractTestFile.tar";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("tar", testFilePath, destFilePath);
		File extractFile = new File(destFilePath + "/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test
	public void testTarGZipFile() throws Exception {
		String testFilePath = testFileDir + "/extract-file-test.tar.gz";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		try {
			ExtractArchives.extract("tgz", testFilePath, destFilePath);
		} catch (Exception e) {
			e.printStackTrace();
		}

		File extractFile = new File(destFilePath + "/extract-file-test/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test
	public void testZipSingleFile() throws Exception {
		String testFilePath = testFileDir + "/extractTestFile.zip";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("zip", testFilePath, destFilePath);
		File extractFile = new File(destFilePath + "/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test
	public void testZipDir() throws Exception {
		String testFilePath = testFileDir + "/extract-file-test.zip";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("zip", testFilePath, destFilePath);
		File extractDir = new File(destFilePath + "/extract-file-test");
		assert extractDir.exists();
		File extractFile = new File(destFilePath + "/extract-file-test/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test
	public void testTarDir() throws Exception {
		String testFilePath = testFileDir + "/extract-file-test.tar";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("tar", testFilePath, destFilePath);
		File extractDir = new File(destFilePath + "/extract-file-test");
		assert extractDir.exists();
		File extractFile = new File(destFilePath + "/extract-file-test/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test
	public void testTarZipDir() throws Exception {
		String testFilePath = testFileDir + "/extract-file-test.tar.gz";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("tgz", testFilePath, destFilePath);
		File extractDir = new File(destFilePath + "/extract-file-test");
		assert extractDir.exists();
		File extractFile = new File(destFilePath + "/extract-file-test/extractTestFile.txt");
		assert extractFile.exists();

		// clean up
		FileUtils.deleteDirectory(new File(destFilePath));
	}

	@Test (expected = ReflexCommonException.class)
	public void testInvalidFileType() throws Exception {
		String testFilePath = testFileDir + "/extract-file-test.tar.gz";
		String destFilePath = testFileDir + "/" + UUID.randomUUID();

		ExtractArchives.extract("invalidType", testFilePath, destFilePath);
	}
}

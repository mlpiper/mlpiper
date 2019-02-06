package com.parallelmachines.reflex.common;

import com.parallelmachines.reflex.common.enums.ArchiveType;
import com.parallelmachines.reflex.common.exceptions.ReflexCommonException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Array;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ExtractArchives {
	private static final Logger logger = Logger.getLogger(ExtractArchives.class);

	/**
	 * This extracts files which get created by "tar cvfz".
	 * it untars them from an arrayByte stream
	 * @param destDirPath
	 * @param arrayByteObj
	 * @throws ReflexCommonException
	 */
	public static String extractTarGZipObj(String destDirPath, ByteArrayInputStream arrayByteObj) throws ReflexCommonException {
		File destDir = new File(destDirPath);
		String mainPath ="";
		try (TarArchiveInputStream tarInput = new TarArchiveInputStream(new GZIPInputStream(arrayByteObj))) {
			TarArchiveEntry currentEntry;
			boolean firstStep = true;
			while ((currentEntry = tarInput.getNextTarEntry()) != null) {
				if (firstStep) {
					firstStep = false;
					mainPath = currentEntry.getName();
				}
				if (currentEntry.isDirectory()) {
					continue;
				}
				File curFile = new File(destDir, currentEntry.getName());

				writeToFile(tarInput, curFile);
			}
		} catch (IOException e) {
			logger.error("IOException", e);
			throw new ReflexCommonException("Failed to extract a tgz file " + e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception", e);
			throw new ReflexCommonException("Failed to extract a tgz file " + e.getMessage());
		}
		return mainPath;
	}



		/**
		 * Extract a tar/zip file to its destination path
		 * @param fileType (tar/zip/tgz)
		 * @param tarFilePath
		 * @param destDirPath
		 * @throws ReflexCommonException
		 */
		public static void extract(String fileType, String tarFilePath, String destDirPath) throws ReflexCommonException {
		validate(fileType, tarFilePath, destDirPath);

		ArchiveType archiveType = ArchiveType.fromString(fileType);
		switch (archiveType) {
			case TAR:
				extractTar(tarFilePath, destDirPath);
				break;
			case ZIP:
				extractZip(tarFilePath, destDirPath);
				break;
			case TGZ:
				extractTarGZip(tarFilePath, destDirPath);
				break;
			case GZIP:
				extractGZip(tarFilePath, destDirPath);
				break;
			case UNKNOWN:
			default:
				throw new ReflexCommonException("Invalid archive type - " + fileType);
		}
	}

	/**
	 * Validate all the input arguments
	 * @param fileType
	 * @param tarFilePath
	 * @param destDirPath
	 * @throws ReflexCommonException
	 */
	private static void validate(String fileType, String tarFilePath, String destDirPath) throws ReflexCommonException {
		String errMsg;
		if (fileType == null) {
			errMsg = "Empty file type " + fileType;
			logger.error(errMsg);
			throw new ReflexCommonException(errMsg);
		}

		if (tarFilePath == null) {
			errMsg = "Empty tar file path " + tarFilePath;
			logger.error(errMsg);
			throw new ReflexCommonException(errMsg);
		}

		if (destDirPath == null) {
			errMsg = "Empty destination file path " + destDirPath;
			logger.error(errMsg);
			throw new ReflexCommonException(errMsg);
		}
	}

	/**
	 * This extracts tar files which get created by command "tar -cvf"
	 * @param tarFilePath
	 * @param destDirPath (If the path did not exist, it will create all its parent dirs)
	 * @throws ReflexCommonException
	 */
	private static void extractTar(String tarFilePath, String destDirPath) throws ReflexCommonException {
		File destDir = new File(destDirPath);
		try (TarArchiveInputStream tarInput = new TarArchiveInputStream(new FileInputStream(tarFilePath))) {
			TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
			if (currentEntry == null) {
				logger.error("Tarball file is invalid! No available tarball entries, probably due to a " +
						"corrupted file! path: " + tarFilePath);
				throw new ReflexCommonException("Tarball file is invalid!");
			}

			do {
				File curFile = new File(destDir, currentEntry.getName());
				if (currentEntry.isDirectory()) {
					FileUtils.forceMkdir(curFile);
				} else {
					writeToFile(tarInput, curFile);
				}
			} while ((currentEntry = tarInput.getNextTarEntry()) != null);
		} catch (Exception e) {
			String msg = String.format("Failed to extract tar file! path=%s, err=%s", tarFilePath, e);
			logger.error(msg);
			throw new ReflexCommonException(msg);
		}
	}

	/**
	 * This extracts zip files
	 * @param tarFilePath
	 * @param destDirPath
	 * @throws ReflexCommonException
	 */
	private static void extractZip(String tarFilePath, String destDirPath) throws ReflexCommonException {
		File destDir = new File(destDirPath);
		try (ZipInputStream zipInput = new ZipInputStream(new FileInputStream(tarFilePath))) {
			ZipEntry currentEntry;
			while((currentEntry = zipInput.getNextEntry()) != null) {
				File curFile = new File(destDir, currentEntry.getName());
				if (currentEntry.isDirectory()) {
					FileUtils.forceMkdir(curFile);
				} else {
					writeToFile(zipInput, curFile);
				}
			}
		} catch (IOException e) {
			logger.error("IOException", e);
			throw new ReflexCommonException("Failed to extract a zip file " + e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception", e);
			throw new ReflexCommonException("Failed to extract a zip file " + e.getMessage());
		}
	}

	/**
	 * This extracts files which get created by "tar cvfz". This one did not work for files which are zipping by "gzip"
	 * "gzip" only zips single file not a directory
	 * @param tarFilePath
	 * @param destDirPath
	 * @throws ReflexCommonException
	 */
	private static void extractTarGZip(String tarFilePath, String destDirPath) throws ReflexCommonException {
		File destDir = new File(destDirPath);
		try (TarArchiveInputStream tarInput = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(tarFilePath)))) {
			TarArchiveEntry currentEntry;

			while ((currentEntry = tarInput.getNextTarEntry()) != null) {
				if (currentEntry.isDirectory()) {
					continue;
				}
				File curFile = new File(destDir, currentEntry.getName());

				writeToFile(tarInput, curFile);
			}
		} catch (IOException e) {
			logger.error("IOException", e);
			throw new ReflexCommonException("Failed to extract a tgz file " + e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception", e);
			throw new ReflexCommonException("Failed to extract a tgz file " + e.getMessage());
		}
	}

	/**
	 * This extract the gzip file first, then untar the extracted gzip file
	 * @param tarFilePath
	 * @param destDirPath
	 * @throws ReflexCommonException
	 */
	private static void extractGZip(String tarFilePath, String destDirPath) throws ReflexCommonException {
		try (GZIPInputStream gzipInputStream = new GZIPInputStream((new FileInputStream(tarFilePath)));
			 FileOutputStream fileOutputStream = new FileOutputStream(destDirPath)) {
			IOUtils.copy(gzipInputStream, fileOutputStream);
		} catch (IOException e) {
			logger.error("IOException", e);
			throw new ReflexCommonException("Failed to extract a gzip file " + e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception", e);
			throw new ReflexCommonException("Failed to extract a gzip file " + e.getMessage());
		}
	}

	private static void writeToFile(InputStream inputStream, File curFile) throws ReflexCommonException {
		try {
			File parent = curFile.getParentFile();
			if (!parent.exists()) {
				parent.mkdirs();
			}

			try (OutputStream outputStream = new FileOutputStream(curFile)) {
				IOUtils.copy(inputStream, outputStream );
			}
		} catch (IOException e) {
			logger.error("IOException", e);
			throw new ReflexCommonException(e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception", e);
			throw new ReflexCommonException(e.getMessage());
		}
	}

	public static void main(String[] args) throws IOException {
		// Internal use only
	}
}

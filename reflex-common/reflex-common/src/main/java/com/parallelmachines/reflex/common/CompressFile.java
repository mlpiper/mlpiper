package com.parallelmachines.reflex.common;

import com.parallelmachines.reflex.common.exceptions.ReflexCommonException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

public class CompressFile {
	private static final Logger logger = Logger.getLogger(CompressFile.class);

	/**
	 * Tar and GZIP the given file (single file only, it will not work for directories)
	 * @param src Path needs to be tar and gzip
	 * @return path tar-gzip format of the srcFile
	 * @throws ReflexCommonException
	 */
	public static Path tarAndGzipFile(Path src) throws ReflexCommonException {
		Path tarPath = Paths.get(src.getParent().toAbsolutePath().toString(), src.getFileName() + ".tar.gz");
		File srcFile = src.toFile();
		try (FileOutputStream fileOutputStream = new FileOutputStream(tarPath.toFile());
			 GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new BufferedOutputStream(fileOutputStream));
			 TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(gzipOutputStream);
			 FileInputStream fileInputStream = new FileInputStream(srcFile);
			 BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {

			tarArchiveOutputStream.putArchiveEntry(new TarArchiveEntry(srcFile, srcFile.getName()));
			IOUtils.copy(bufferedInputStream, tarArchiveOutputStream);
			tarArchiveOutputStream.closeArchiveEntry();
			return tarPath;
		} catch (IOException e) {
			String errMsg = "Failed to tar and gzip file [" + src.toAbsolutePath().toString() + "]";
			logger.error(errMsg, e);
			throw new ReflexCommonException(errMsg);
		}
	}
}

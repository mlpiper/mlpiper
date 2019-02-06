package com.parallelmachines.reflex.common;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.zip.*;


public class DirectoryPack {
    private static final Logger logger = Logger.getLogger(DirectoryPack.class);

    private File sourceGzip;
    private File destGzip;

    private int BUFFER_LENGTH = 1024;  // While converting gzip to byte stream 1024 bytes will be block size
    private int GZ_EXTENTION_LEN = 3;  // Constant to store length of .gz
    private String tempFile = UUID.randomUUID().toString();

    /**
     * Constructor which gives choice to caller to specify temp folder
     * @param tmpFolder Folder which will be temporarily used to store .tar.gz file
     * */
    public DirectoryPack(String tmpFolder) {
        this.sourceGzip = new File(FilenameUtils.concat(tmpFolder, this.tempFile + ".tar.gz"));
        this.destGzip = new File("dest-" + tempFile + ".tar.gz");
    }

    /**
     * Constructor which use /tmp as temp folder
     * */
    public DirectoryPack() {
        this.sourceGzip = new File(FilenameUtils.concat(File.separator + "tmp",
               this.tempFile + ".tar.gz"));
        this.destGzip = new File("dest-" + tempFile + ".tar.gz");
    }

    /** Add files into tar
     * @param tOut          Tar archive output stream
     * @param path          Path of folder from which all files need to be tared
     * @throws IOException  If an input or output exception occurred
     */
    private void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base)
            throws IOException
    {
        File f = new File(path);
        String entryName =  base + f.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
        tOut.putArchiveEntry(tarEntry);

        if (f.isFile()) {
            // Set the entry size to be the file size
            tarEntry.setSize(f.length());
            IOUtils.copy(new FileInputStream(f), tOut);
            tOut.closeArchiveEntry();
        } else {
            tOut.closeArchiveEntry();
            File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFileToTarGz(tOut, child.getAbsolutePath(), entryName + File.separator);
                }
            }
        }
    }

    /** Convert all files in folder to gzip
     * @param sourcePath    Path of folder from which all files need to be gun zipped
     * @param gZipFilePath  Name of file to be given to gzip file
     * @throws IOException  If an input or output exception occurred
     */
    public void gZipIt(String sourcePath, String gZipFilePath) throws IOException {

        // First convert file into tar
        String dirPath = sourcePath;
        String tarGzPath = gZipFilePath;
        FileOutputStream fOut = new FileOutputStream(new File(tarGzPath));
        BufferedOutputStream bOut = new BufferedOutputStream(fOut);
        GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bOut);
        TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut);
        tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

        // Now add files into gzip
        this.addFileToTarGz(tOut, dirPath, "");

        tOut.finish();
        tOut.close();
        gzOut.close();
        bOut.close();
        fOut.close();
    }

    /**
     * Pack a directory or file and return the tarball file path
     * @param sourceDirPath Directory/File to pack
     * @return The tarball file path
     * @throws IOException
     */
    private File packAndScheduleForDeletion(String sourceDirPath) throws IOException {
        File modelSourcePathFile = new File(sourceDirPath);
        if (!modelSourcePathFile.exists() || !modelSourcePathFile.canRead()) {
            String msg = String.format("Directory [%s] does not exist or is not readable",
                    sourceDirPath);
            logger.error(msg);
            throw new IOException("Exception occurred while packing directory, Can't find directory: " + sourceDirPath);
        }
        this.gZipIt(sourceDirPath, this.sourceGzip.getAbsolutePath());
        logger.info(String.format("Directory was packed successfully! srcDir=%s, packedFile=%s", sourceDirPath, this.sourceGzip));

        FileUtils.forceDeleteOnExit(this.sourceGzip);

        return this.sourceGzip;
    }

    /** Pack contents of all files in folder to byte array
     * @param sourceDirPath     Path of folder from which all files need to be gun zipped
     * @throws IOException      If an input or output exception occurred
     * @return                  Byte array
     */
    public byte[] pack(String sourceDirPath) throws IOException {

        File gZipSrcFile = packAndScheduleForDeletion(sourceDirPath);

        //Convert zip into stream
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[BUFFER_LENGTH];

        FileInputStream fin = new FileInputStream(gZipSrcFile);

        int length;
        while ((length = fin.read(buffer)) > 0) {
            bos.write(buffer, 0, length);
        }

        logger.debug("Packed byte array of size:"+length);

        fin.close();
        bos.close();

        deleteFile(this.sourceGzip);

        return bos.toByteArray();
    }

    static private void deleteFile(File fileToDelete) {
        try {
            Files.delete(fileToDelete.toPath());
        } catch (IOException e) {
            logger.warn(String.format("Failed to delete file! Consider deleting it manually! path: %s, message: %s",
                    fileToDelete.getAbsolutePath(), e.getMessage()));
        }
    }

    /** Un archive all files from tar file in to a folder
     * @param gZipTar                   Name of tar file to be untared
     * @param destPath                  Path of folder where user want to un tar
     * @throws FileNotFoundException    If any file not found exception occurred
     * @throws IOException              If an input or output exception occurred
     * @throws ArchiveException         If any archive(Tar, GZIP) exception occurred
     */
    private void unTar(String gZipTar, String destPath) throws FileNotFoundException, IOException, ArchiveException {
        String tarFile = FilenameUtils.concat(destPath, gZipTar);

        logger.info("Untarring: " + tarFile);
        InputStream is = new FileInputStream(tarFile);
        ArchiveInputStream input = new ArchiveStreamFactory()
                .createArchiveInputStream(ArchiveStreamFactory.TAR, is);
        ArchiveEntry entry;

        while ((entry = input.getNextEntry()) != null) {
            File outputFile = new File(destPath, entry.getName());
            if (entry.isDirectory()) {
                if (!outputFile.exists()) {
                    if (!outputFile.mkdirs()) {
                        throw new IllegalStateException(String.format("Couldn't create directory %s.",
                                outputFile.getAbsolutePath()));
                    }
                }
            } else {
                OutputStream outputFileStream = new FileOutputStream(outputFile);
                IOUtils.copy(input, outputFileStream);

                outputFileStream.close();
            }
        }

        input.close();
        is.close();
    }

    /** Unzip all files in folder to gzip
     * @param gZipFilePath      Path of file which need to be unarchived
     * @param destPath          Path of directory where all files will be placed
     * @throws IOException      If an input or output exception occurred
     */
    public void unGZip(String gZipFilePath, String destPath) throws Exception {
        logger.info("Unzipping files from: " + gZipFilePath + ", may take some time depending on file size");

        File inputFile = new File(gZipFilePath);
        File outputDir = new File(destPath);

        // ASSUMING FILE WILL END WITH .gz
        String tarFile = inputFile.getName().substring(0, inputFile.getName().length() - this.GZ_EXTENTION_LEN);
        final File outputFile = new File(outputDir, tarFile);

        final GZIPInputStream in = new GZIPInputStream(new FileInputStream(inputFile));
        final FileOutputStream out = new FileOutputStream(outputFile);

        IOUtils.copy(in, out);

        in.close();
        out.close();
        logger.info("gunzip part done, doing untar, tarfile " + tarFile + " destPath: " + destPath);
        try {
            this.unTar(tarFile, destPath);
            logger.info("Unpacked files at: " + destPath);
        } catch (Exception e) {
            logger.error(e);
            throw new Exception("Exception occurred while un archiving: " + e);
        }

        if (!outputFile.delete()) {
            logger.warn("Failed to delete an intermediate archive file! path=" + outputFile.getAbsolutePath());
        }
    }

    /**
     * Unpack tarball file and then delete it
     * @param gZipFilePath The tarbal file path to unpack
     * @param destDirPath The destination folder, where to unpack the given tarball
     * @throws Exception
     */
    public void unPackFileAndDelete(String gZipFilePath, String destDirPath) throws Exception {
        logger.debug("Unpacking file, this may take some time depending on file size, file=" + gZipFilePath);

        File modelDestPathFile = new File(destDirPath);
        if (!modelDestPathFile.exists() || !modelDestPathFile.canRead()) {
            String msg = String.format("Directory specified [%s] does not exist or is not readable", destDirPath);
            logger.error(msg);
            throw new IOException(msg);
        }

        // Un-archive gzip and then un-tar tar into dest folder
        this.unGZip(gZipFilePath, destDirPath);
        logger.info("Archive was unpacked to: " + destDirPath);

        if (!new File(gZipFilePath).delete()) {
            logger.warn("Failed to delete an archive file! path=" + gZipFilePath);
        }
    }

    /** Convert byte array into folder
     * @param byteArray     Byte array passed to reconstruct folder
     * @param destDirPath   Path of directory where all files will be placed
     * @throws IOException  If an input or output exception occurred
     */
    public void unPack(byte[] byteArray, String destDirPath) throws Exception {
        String dGzip = Paths.get(destDirPath, this.destGzip.toString()).toString();
        File f = new File(dGzip);
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(byteArray);
        fos.flush();
        fos.close();

        unPackFileAndDelete(dGzip, destDirPath);
    }

    public static void main(String args[]) {
        // Intentionally left blank
    }
}

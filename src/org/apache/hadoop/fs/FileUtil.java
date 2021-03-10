//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.jar.Attributes.Name;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.io.nativeio.NativeIO.Windows;
import org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

@Public
@Evolving
public class FileUtil {
    private static final Log LOG = LogFactory.getLog(FileUtil.class);
    public static final int SYMLINK_NO_PRIVILEGE = 2;

    public FileUtil() {
    }

    public static Path[] stat2Paths(FileStatus[] stats) {
        if (stats == null) {
            return null;
        } else {
            Path[] ret = new Path[stats.length];

            for(int i = 0; i < stats.length; ++i) {
                ret[i] = stats[i].getPath();
            }

            return ret;
        }
    }

    public static Path[] stat2Paths(FileStatus[] stats, Path path) {
        return stats == null ? new Path[]{path} : stat2Paths(stats);
    }

    public static boolean fullyDelete(File dir) {
        return fullyDelete(dir, false);
    }

    public static boolean fullyDelete(File dir, boolean tryGrantPermissions) {
        if (tryGrantPermissions) {
            File parent = dir.getParentFile();
            grantPermissions(parent);
        }

        if (deleteImpl(dir, false)) {
            return true;
        } else {
            return !fullyDeleteContents(dir, tryGrantPermissions) ? false : deleteImpl(dir, true);
        }
    }

    public static String readLink(File f) {
        try {
            return Shell.execCommand(Shell.getReadlinkCommand(f.toString())).trim();
        } catch (IOException var2) {
            return "";
        }
    }

    private static void grantPermissions(File f) {
        setExecutable(f, true);
        setReadable(f, true);
        setWritable(f, true);
    }

    private static boolean deleteImpl(File f, boolean doLog) {
        if (f == null) {
            LOG.warn("null file argument.");
            return false;
        } else {
            boolean wasDeleted = f.delete();
            if (wasDeleted) {
                return true;
            } else {
                boolean ex = f.exists();
                if (doLog && ex) {
                    LOG.warn("Failed to delete file or dir [" + f.getAbsolutePath() + "]: it still exists.");
                }

                return !ex;
            }
        }
    }

    public static boolean fullyDeleteContents(File dir) {
        return fullyDeleteContents(dir, false);
    }

    public static boolean fullyDeleteContents(File dir, boolean tryGrantPermissions) {
        if (tryGrantPermissions) {
            grantPermissions(dir);
        }

        boolean deletionSucceeded = true;
        File[] contents = dir.listFiles();
        if (contents != null) {
            for(int i = 0; i < contents.length; ++i) {
                if (contents[i].isFile()) {
                    if (!deleteImpl(contents[i], true)) {
                        deletionSucceeded = false;
                    }
                } else {
                    boolean b = false;
                    b = deleteImpl(contents[i], false);
                    if (!b && !fullyDelete(contents[i], tryGrantPermissions)) {
                        deletionSucceeded = false;
                    }
                }
            }
        }

        return deletionSucceeded;
    }

    /** @deprecated */
    @Deprecated
    public static void fullyDelete(FileSystem fs, Path dir) throws IOException {
        fs.delete(dir, true);
    }

    private static void checkDependencies(FileSystem srcFS, Path src, FileSystem dstFS, Path dst) throws IOException {
        if (srcFS == dstFS) {
            String srcq = src.makeQualified(srcFS).toString() + "/";
            String dstq = dst.makeQualified(dstFS).toString() + "/";
            if (dstq.startsWith(srcq)) {
                if (srcq.length() == dstq.length()) {
                    throw new IOException("Cannot copy " + src + " to itself.");
                }

                throw new IOException("Cannot copy " + src + " to its subdirectory " + dst);
            }
        }

    }

    public static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf) throws IOException {
        return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
    }

    public static boolean copy(FileSystem srcFS, Path[] srcs, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf) throws IOException {
        boolean gotException = false;
        boolean returnVal = true;
        StringBuilder exceptions = new StringBuilder();
        if (srcs.length == 1) {
            return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);
        } else if (!dstFS.exists(dst)) {
            throw new IOException("`" + dst + "': specified destination directory " + "does not exist");
        } else {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (!sdst.isDirectory()) {
                throw new IOException("copying multiple files, but last argument `" + dst + "' is not a directory");
            } else {
                Path[] var16 = srcs;
                int var11 = srcs.length;

                for(int var12 = 0; var12 < var11; ++var12) {
                    Path src = var16[var12];

                    try {
                        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf)) {
                            returnVal = false;
                        }
                    } catch (IOException var15) {
                        gotException = true;
                        exceptions.append(var15.getMessage());
                        exceptions.append("\n");
                    }
                }

                if (gotException) {
                    throw new IOException(exceptions.toString());
                } else {
                    return returnVal;
                }
            }
        }
    }

    public static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf) throws IOException {
        FileStatus fileStatus = srcFS.getFileStatus(src);
        return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
    }

    public static boolean copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf) throws IOException {
        Path src = srcStatus.getPath();
        dst = checkDest(src.getName(), dstFS, dst, overwrite);
        FileStatus[] contents;
        if (srcStatus.isDirectory()) {
            checkDependencies(srcFS, src, dstFS, dst);
            if (!dstFS.mkdirs(dst)) {
                return false;
            }

            contents = srcFS.listStatus(src);

            for(int i = 0; i < contents.length; ++i) {
                copy(srcFS, contents[i], dstFS, new Path(dst, contents[i].getPath().getName()), deleteSource, overwrite, conf);
            }
        } else {
            contents = null;
            FSDataOutputStream out = null;

            try {
                InputStream in = srcFS.open(src);
                out = dstFS.create(dst, overwrite);
                IOUtils.copyBytes(in, out, conf, true);
            } catch (IOException var11) {
                IOUtils.closeStream(out);
//                IOUtils.closeStream(contents);
                throw var11;
            }
        }

        return deleteSource ? srcFS.delete(src, true) : true;
    }

    public static boolean copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String addString) throws IOException {
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDirectory()) {
            return false;
        } else {
            FSDataOutputStream out = dstFS.create(dstFile);

            try {
                FileStatus[] contents = srcFS.listStatus(srcDir);
                Arrays.sort(contents);

                for(int i = 0; i < contents.length; ++i) {
                    if (contents[i].isFile()) {
                        FSDataInputStream in = srcFS.open(contents[i].getPath());

                        try {
                            IOUtils.copyBytes(in, out, conf, false);
                            if (addString != null) {
                                out.write(addString.getBytes("UTF-8"));
                            }
                        } finally {
                            in.close();
                        }
                    }
                }
            } finally {
                out.close();
            }

            return deleteSource ? srcFS.delete(srcDir, true) : true;
        }
    }

    public static boolean copy(File src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf) throws IOException {
        dst = checkDest(src.getName(), dstFS, dst, false);
        File[] contents;
        if (src.isDirectory()) {
            if (!dstFS.mkdirs(dst)) {
                return false;
            }

            contents = listFiles(src);

            for(int i = 0; i < contents.length; ++i) {
                copy(contents[i], dstFS, new Path(dst, contents[i].getName()), deleteSource, conf);
            }
        } else {
            if (!src.isFile()) {
                throw new IOException(src.toString() + ": No such file or directory");
            }

            contents = null;
            FSDataOutputStream out = null;

            try {
                InputStream in = new FileInputStream(src);
                out = dstFS.create(dst);
                IOUtils.copyBytes(in, out, conf);
            } catch (IOException var8) {
                IOUtils.closeStream(out);
//                IOUtils.closeStream(contents);
                throw var8;
            }
        }

        return deleteSource ? fullyDelete(src) : true;
    }

    public static boolean copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf) throws IOException {
        FileStatus filestatus = srcFS.getFileStatus(src);
        return copy(srcFS, filestatus, dst, deleteSource, conf);
    }

    private static boolean copy(FileSystem srcFS, FileStatus srcStatus, File dst, boolean deleteSource, Configuration conf) throws IOException {
        Path src = srcStatus.getPath();
        if (srcStatus.isDirectory()) {
            if (!dst.mkdirs()) {
                return false;
            }

            FileStatus[] contents = srcFS.listStatus(src);

            for(int i = 0; i < contents.length; ++i) {
                copy(srcFS, contents[i], new File(dst, contents[i].getPath().getName()), deleteSource, conf);
            }
        } else {
            InputStream in = srcFS.open(src);
            IOUtils.copyBytes(in, new FileOutputStream(dst), conf);
        }

        return deleteSource ? srcFS.delete(src, true) : true;
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }

                return checkDest((String)null, dstFS, new Path(dst, srcName), overwrite);
            }

            if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }

        return dst;
    }

    public static String makeShellPath(String filename) throws IOException {
        return filename;
    }

    public static String makeShellPath(File file) throws IOException {
        return makeShellPath(file, false);
    }

    public static String makeShellPath(File file, boolean makeCanonicalPath) throws IOException {
        return makeCanonicalPath ? makeShellPath(file.getCanonicalPath()) : makeShellPath(file.toString());
    }

    public static long getDU(File dir) {
        long size = 0L;
        if (!dir.exists()) {
            return 0L;
        } else if (!dir.isDirectory()) {
            return dir.length();
        } else {
            File[] allFiles = dir.listFiles();
            if (allFiles != null) {
                for(int i = 0; i < allFiles.length; ++i) {
                    boolean isSymLink;
                    try {
                        isSymLink = FileUtils.isSymlink(allFiles[i]);
                    } catch (IOException var7) {
                        isSymLink = true;
                    }

                    if (!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }

            return size;
        }
    }

    public static void unZip(File inFile, File unzipDir) throws IOException {
        ZipFile zipFile = new ZipFile(inFile);

        try {
            Enumeration entries = zipFile.entries();

            while(entries.hasMoreElements()) {
                ZipEntry entry = (ZipEntry)entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = zipFile.getInputStream(entry);

                    try {
                        File file = new File(unzipDir, entry.getName());
                        if (!file.getParentFile().mkdirs() && !file.getParentFile().isDirectory()) {
                            throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
                        }

                        FileOutputStream out = new FileOutputStream(file);

                        try {
                            byte[] buffer = new byte[8192];

                            int i;
                            while((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }

    }

    public static void unTar(File inFile, File untarDir) throws IOException {
        if (!untarDir.mkdirs() && !untarDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + untarDir);
        } else {
            boolean gzipped = inFile.toString().endsWith("gz");
            if (Shell.WINDOWS) {
                unTarUsingJava(inFile, untarDir, gzipped);
            } else {
                unTarUsingTar(inFile, untarDir, gzipped);
            }

        }
    }

    private static void unTarUsingTar(File inFile, File untarDir, boolean gzipped) throws IOException {
        StringBuffer untarCommand = new StringBuffer();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(makeShellPath(inFile));
            untarCommand.append("' | (");
        }

        untarCommand.append("cd '");
        untarCommand.append(makeShellPath(untarDir));
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");
        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(makeShellPath(inFile));
        }

        String[] shellCmd = new String[]{"bash", "-c", untarCommand.toString()};
        ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile + ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir, boolean gzipped) throws IOException {
        InputStream inputStream = null;
        TarArchiveInputStream tis = null;

        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }

            tis = new TarArchiveInputStream(inputStream);

            for(TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; entry = tis.getNextTarEntry()) {
                unpackEntries(tis, entry, untarDir);
            }
        } finally {
            IOUtils.cleanup(LOG, new Closeable[]{tis, inputStream});
        }

    }

    private static void unpackEntries(TarArchiveInputStream tis, TarArchiveEntry entry, File outputDir) throws IOException {
        File subDir;
        if (entry.isDirectory()) {
            subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdirs() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
            } else {
                TarArchiveEntry[] var8 = entry.getDirectoryEntries();
                int var9 = var8.length;

                for(int var10 = 0; var10 < var9; ++var10) {
                    TarArchiveEntry e = var8[var10];
                    unpackEntries(tis, e, subDir);
                }

            }
        } else {
            subDir = new File(outputDir, entry.getName());
            if (!subDir.getParentFile().exists() && !subDir.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
            } else {
                byte[] data = new byte[2048];
                BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(subDir));

                int count;
                while((count = tis.read(data)) != -1) {
                    outputStream.write(data, 0, count);
                }

                outputStream.flush();
                outputStream.close();
            }
        }
    }

    public static int symLink(String target, String linkname) throws IOException {
        File targetFile = new File(Path.getPathWithoutSchemeAndAuthority(new Path(target)).toString());
        File linkFile = new File(Path.getPathWithoutSchemeAndAuthority(new Path(linkname)).toString());
        if (Shell.WINDOWS && !Shell.isJava7OrAbove() && targetFile.isFile()) {
            try {
                LOG.warn("FileUtil#symlink: On Windows+Java6, copying file instead of creating a symlink. Copying " + target + " -> " + linkname);
                if (!linkFile.getParentFile().exists()) {
                    LOG.warn("Parent directory " + linkFile.getParent() + " does not exist.");
                    return 1;
                } else {
                    FileUtils.copyFile(targetFile, linkFile);
                    return 0;
                }
            } catch (IOException var8) {
                LOG.warn("FileUtil#symlink failed to copy the file with error: " + var8.getMessage());
                return 1;
            }
        } else {
            String[] cmd = Shell.getSymlinkCommand(targetFile.toString(), linkFile.toString());

            ShellCommandExecutor shExec;
            try {
                if (Shell.WINDOWS && linkFile.getParentFile() != null && !(new Path(target)).isAbsolute()) {
                    shExec = new ShellCommandExecutor(cmd, linkFile.getParentFile());
                } else {
                    shExec = new ShellCommandExecutor(cmd);
                }

                shExec.execute();
            } catch (ExitCodeException var9) {
                int returnVal = var9.getExitCode();
                if (Shell.WINDOWS && returnVal == 2) {
                    LOG.warn("Fail to create symbolic links on Windows. The default security settings in Windows disallow non-elevated administrators and all non-administrators from creating symbolic links. This behavior can be changed in the Local Security Policy management console");
                } else if (returnVal != 0) {
                    LOG.warn("Command '" + StringUtils.join(" ", cmd) + "' failed " + returnVal + " with: " + var9.getMessage());
                }

                return returnVal;
            } catch (IOException var10) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Error while create symlink " + linkname + " to " + target + "." + " Exception: " + StringUtils.stringifyException(var10));
                }

                throw var10;
            }

            return shExec.getExitCode();
        }
    }

    public static int chmod(String filename, String perm) throws IOException, InterruptedException {
        return chmod(filename, perm, false);
    }

    public static int chmod(String filename, String perm, boolean recursive) throws IOException {
        String[] cmd = Shell.getSetPermissionCommand(perm, recursive);
        String[] args = new String[cmd.length + 1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length] = (new File(filename)).getPath();
        ShellCommandExecutor shExec = new ShellCommandExecutor(args);

        try {
            shExec.execute();
        } catch (IOException var7) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error while changing permission : " + filename + " Exception: " + StringUtils.stringifyException(var7));
            }
        }

        return shExec.getExitCode();
    }

    public static void setOwner(File file, String username, String groupname) throws IOException {
        if (username == null && groupname == null) {
            throw new IOException("username == null && groupname == null");
        } else {
            String arg = (username == null ? "" : username) + (groupname == null ? "" : ":" + groupname);
            String[] cmd = Shell.getSetOwnerCommand(arg);
            execCommand(file, cmd);
        }
    }

    public static boolean setReadable(File f, boolean readable) {
        if (Shell.WINDOWS) {
            try {
                String permission = readable ? "u+r" : "u-r";
                chmod(f.getCanonicalPath(), permission, false);
                return true;
            } catch (IOException var3) {
                return false;
            }
        } else {
            return true;//f.setReadable(readable);
        }
    }

    public static boolean setWritable(File f, boolean writable) {
        if (Shell.WINDOWS) {
            try {
                String permission = writable ? "u+w" : "u-w";
                chmod(f.getCanonicalPath(), permission, false);
                return true;
            } catch (IOException var3) {
                return false;
            }
        } else {
            return true;
        }
    }

    public static boolean setExecutable(File f, boolean executable) {
        if (Shell.WINDOWS) {
            try {
                String permission = executable ? "u+x" : "u-x";
                chmod(f.getCanonicalPath(), permission, false);
                return true;
            } catch (IOException var3) {
                return false;
            }
        } else {
            return true;//f.setExecutable(executable);
        }
    }

    public static boolean canRead(File f) {
        return true;
    }

    public static boolean canWrite(File f) {
        return true;
    }

    public static boolean canExecute(File f) {
        return true;
    }

    public static void setPermission(File f, FsPermission permission) throws IOException {
        FsAction user = permission.getUserAction();
        FsAction group = permission.getGroupAction();
        FsAction other = permission.getOtherAction();
        if (group == other && !NativeIO.isAvailable() && !Shell.WINDOWS) {
            boolean rv = true;
            rv = f.setReadable(group.implies(FsAction.READ), false);
            checkReturnValue(rv, f, permission);
            if (group.implies(FsAction.READ) != user.implies(FsAction.READ)) {
                rv = f.setReadable(user.implies(FsAction.READ), true);
                checkReturnValue(rv, f, permission);
            }

            rv = f.setWritable(group.implies(FsAction.WRITE), false);
            checkReturnValue(rv, f, permission);
            if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE)) {
                rv = f.setWritable(user.implies(FsAction.WRITE), true);
                checkReturnValue(rv, f, permission);
            }

            rv = f.setExecutable(group.implies(FsAction.EXECUTE), false);
            checkReturnValue(rv, f, permission);
            if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE)) {
                rv = f.setExecutable(user.implies(FsAction.EXECUTE), true);
                checkReturnValue(rv, f, permission);
            }

        } else {
            execSetPermission(f, permission);
        }
    }

    private static void checkReturnValue(boolean rv, File p, FsPermission permission) throws IOException {
        if (!rv) {
            throw new IOException("Failed to set permissions of path: " + p + " to " + String.format("%04o", permission.toShort()));
        }
    }

    private static void execSetPermission(File f, FsPermission permission) throws IOException {
        if (NativeIO.isAvailable()) {
            POSIX.chmod(f.getCanonicalPath(), permission.toShort());
        } else {
            execCommand(f, Shell.getSetPermissionCommand(String.format("%04o", permission.toShort()), false));
        }

    }

    static String execCommand(File f, String... cmd) throws IOException {
        String[] args = new String[cmd.length + 1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length] = f.getCanonicalPath();
        String output = Shell.execCommand(args);
        return output;
    }

    public static final File createLocalTempFile(File basefile, String prefix, boolean isDeleteOnExit) throws IOException {
        File tmp = File.createTempFile(prefix + basefile.getName(), "", basefile.getParentFile());
        if (isDeleteOnExit) {
            tmp.deleteOnExit();
        }

        return tmp;
    }

    public static void replaceFile(File src, File target) throws IOException {
        if (!src.renameTo(target)) {
            int var2 = 5;

            while(target.exists() && !target.delete() && var2-- >= 0) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException var4) {
                    throw new IOException("replaceFile interrupted.");
                }
            }

            if (!src.renameTo(target)) {
                throw new IOException("Unable to rename " + src + " to " + target);
            }
        }

    }

    public static File[] listFiles(File dir) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
        } else {
            return files;
        }
    }

    public static String[] list(File dir) throws IOException {
        String[] fileNames = dir.list();
        if (fileNames == null) {
            throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
        } else {
            return fileNames;
        }
    }

    public static String[] createJarWithClassPath(String inputClassPath, Path pwd, Map<String, String> callerEnv) throws IOException {
        return createJarWithClassPath(inputClassPath, pwd, pwd, callerEnv);
    }

    public static String[] createJarWithClassPath(String inputClassPath, Path pwd, Path targetDir, Map<String, String> callerEnv) throws IOException {
        Map<String, String> env = Shell.WINDOWS ? new CaseInsensitiveMap(callerEnv) : callerEnv;
        String[] classPathEntries = inputClassPath.split(File.pathSeparator);

        for(int i = 0; i < classPathEntries.length; ++i) {
            classPathEntries[i] = StringUtils.replaceTokens(classPathEntries[i], StringUtils.ENV_VAR_PATTERN, (Map)env);
        }

        File workingDir = new File(pwd.toString());
        if (!workingDir.mkdirs()) {
            LOG.debug("mkdirs false for " + workingDir + ", execution will continue");
        }

        StringBuilder unexpandedWildcardClasspath = new StringBuilder();
        List<String> classPathEntryList = new ArrayList(classPathEntries.length);
        String[] var9 = classPathEntries;
        int var10 = classPathEntries.length;

        for(int var11 = 0; var11 < var10; ++var11) {
            String classPathEntry = var9[var11];
            if (classPathEntry.length() != 0) {
                if (!classPathEntry.endsWith("*")) {
                    File fileCpEntry = null;
                    if (!(new Path(classPathEntry)).isAbsolute()) {
                        fileCpEntry = new File(targetDir.toString(), classPathEntry);
                    } else {
                        fileCpEntry = new File(classPathEntry);
                    }

                    String classPathEntryUrl = fileCpEntry.toURI().toURL().toExternalForm();
                    if (classPathEntry.endsWith("/") && !classPathEntryUrl.endsWith("/")) {
                        classPathEntryUrl = classPathEntryUrl + "/";
                    }

                    classPathEntryList.add(classPathEntryUrl);
                } else {
                    boolean foundWildCardJar = false;
                    Path globPath = (new Path(classPathEntry)).suffix("{.jar,.JAR}");
                    FileStatus[] wildcardJars = FileContext.getLocalFSFileContext().util().globStatus(globPath);
                    if (wildcardJars != null) {
                        FileStatus[] var16 = wildcardJars;
                        int var17 = wildcardJars.length;

                        for(int var18 = 0; var18 < var17; ++var18) {
                            FileStatus wildcardJar = var16[var18];
                            foundWildCardJar = true;
                            classPathEntryList.add(wildcardJar.getPath().toUri().toURL().toExternalForm());
                        }
                    }

                    if (!foundWildCardJar) {
                        unexpandedWildcardClasspath.append(File.pathSeparator);
                        unexpandedWildcardClasspath.append(classPathEntry);
                    }
                }
            }
        }

        String jarClassPath = StringUtils.join(" ", classPathEntryList);
        Manifest jarManifest = new Manifest();
        jarManifest.getMainAttributes().putValue(Name.MANIFEST_VERSION.toString(), "1.0");
        jarManifest.getMainAttributes().putValue(Name.CLASS_PATH.toString(), jarClassPath);
        File classPathJar = File.createTempFile("classpath-", ".jar", workingDir);
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        JarOutputStream jos = null;

        try {
            fos = new FileOutputStream(classPathJar);
            bos = new BufferedOutputStream(fos);
            jos = new JarOutputStream(bos, jarManifest);
        } finally {
            IOUtils.cleanup(LOG, new Closeable[]{jos, bos, fos});
        }

        String[] var32 = new String[]{classPathJar.getCanonicalPath(), unexpandedWildcardClasspath.toString()};
        return var32;
    }

    /** @deprecated */
    @Deprecated
    public static class HardLink extends org.apache.hadoop.fs.HardLink {
        public HardLink() {
        }
    }
}

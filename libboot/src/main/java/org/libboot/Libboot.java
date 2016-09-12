/*
Copyright 2016 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.libboot;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Libboot
{
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private static final int SPEC_TODO_REWRITE = 10;

    private static final int TODO_DOWNLOAD = 20;
    private static final int TODO_NOTHING = 30;

    private static final int ALL_GOOD = 100;
    private static final int NEEDS_REDOWNLOAD = 200;
    private static final int ERROR_WRONG_ARGS = -1;
    private static final int ERROR_DOWNLOAD_FAILED = -2;
    private static final int ERROR_UNKOWN = -4;
    private static final int NO_ERROR = 0;

    public static void main(String[] args)
    {
        if (args.length == 0)
        {
            System.exit(usageAndExit());
        }
        String command = args[0];
        if (command.equals("download"))
        {
            System.exit(download(args));
        }
        else if (command.equals("clean"))
        {
            System.exit(clean(args));
        }
        else
        {
            System.exit(usageAndExit());
        }
    }

    private static int clean(String[] args)
    {
        throw new RuntimeException("not implemented");
    }

    private static int download(String[] args)
    {
        if (args.length < 3 || args.length > 4)
        {
            return usageAndExit();
        }
        String specFile = args[1];
        String repoFile = args[2];
        int ioThreads = 4;
        if (args.length == 4)
        {
            try
            {
                ioThreads = Integer.parseInt(args[3]);
            }
            catch (NumberFormatException e)
            {
                error("Could not parse # of io threads. '"+args[3]+"' is not an integer.");
                return usageAndExit();
            }
        }
        return new Download(specFile, repoFile, ioThreads).download();
    }

    private static int usageAndExit()
    {
        String x = "Usage:";
        error(x);
        error("Libboot download <specfile> <repoFile> [<# of io threads>]");
        error("Libboot clean <specfile>");
        return ERROR_WRONG_ARGS;
    }

    private static void info(String x)
    {
        System.out.println("INFO: " + x);
    }

    private static void debug(String x)
    {
        System.out.println("DEBUG: " + x);
    }

    private static void error(String x)
    {
        System.err.println("ERROR: " + x);
    }

    private static void warn(String x)
    {
        System.out.println("WARN: " + x);
    }

    private static void closeNoException(Closeable lis)
    {
        try
        {
            if (lis != null) lis.close();
        }
        catch (IOException e)
        {
            //ignore
        }
    }

    private static class Download
    {
        String spec;
        int specToDo = TODO_NOTHING;
        String repo;
        int ioThreads;
        List<Spec> specs = new ArrayList<Spec>();
        Map<String, Repo> repos;
        boolean needsWork;
        LinkedBlockingQueue<Spec> specQueue = new LinkedBlockingQueue<Spec>();
        List<ResultFile> resultFiles = new ArrayList<ResultFile>();
        List<String> errors = new ArrayList<String>();

        public Download(String specFile, String repoFile, int ioThreads)
        {
            this.spec = specFile;
            this.repo = repoFile;
            this.ioThreads = ioThreads;
        }

        public int download()
        {
            try
            {
                readSpecFile();
                findDelta();
                if (needsWork)
                {
                    readRepoFile();
                    processDelta();
                    if (!errors.isEmpty())
                    {
                        for(int i=0;i<errors.size();i++)
                        {
                            error(errors.get(i));
                        }
                        return ERROR_DOWNLOAD_FAILED;
                    }
                }
                if (specToDo == SPEC_TODO_REWRITE)
                {
                    Spec.writeSpecFile(this.spec, this.specs);
                }
            }
            catch (Throwable e)
            {
                error("Unrecoverable error "+printableError(e));
                return ERROR_UNKOWN;
            }
            finally
            {
                for(ResultFile f: resultFiles) f.close();
            }
            return NO_ERROR;
        }

        private void readRepoFile()
        {
            // repoName, urlPattern
            // urlPattern must include {groupIdWithSlashes} {artifactId} {version} {ext} in it.
            // example:
            // central, http://repo.maven.apache.org/maven2/{groupIdWithSlashes}/{version}/{artifactId}-{version}.{ext}
            // nexusMirror, http://mirror.foo.com/service/local/repositories/central/content/{groupIdWithSlashes}/{artifactId}/{version}/{artifactId}-{version}.{ext}
            this.repos = new HashMap<String, Repo>();
            File repoFile = new File(this.repo);
            int lineNum = 0;
            FileInputStream fis = null;
            LineNumberReader lis = null;
            try
            {
                fis = new FileInputStream(repoFile);
                lis = new LineNumberReader(new InputStreamReader(fis));
                String line = lis.readLine();
                StringBuilder sb = new StringBuilder(32);
                while(line != null)
                {
                    lineNum++;
                    line = line.trim();
                    if (!line.startsWith("#") && !line.isEmpty())
                    {
                        Repo repo1 = Repo.parseRepo(line, sb, lineNum);
                        repos.put(repo1.repoName, repo1);
                    }
                    line = lis.readLine();
                }
            }
            catch(FileNotFoundException e)
            {
                throw new RuntimeException("Could not open spec file. File "+this.spec+" was not found", e);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not read spec file around line "+lineNum, e);
            }
            finally
            {
                closeNoException(fis);
            }
        }

        private void processDelta()
        {
            info("Files to download: "+specQueue.size());
            Thread[] threads = new Thread[ioThreads];
            for(int i=0;i<ioThreads;i++)
            {
                threads[i] = new DownloadThread(i + 1);
                threads[i].start();
            }
            for(int i=0;i<ioThreads;i++)
            {
                try
                {
                    threads[i].join();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException("Unexpected interrupt", e);
                }
            }
        }

        private void findDelta()
        {
            Map<String, Map<String, Spec>> dirToSpecMap = organizeSpecs();
            for(Map.Entry<String, Map<String, Spec>> entry: dirToSpecMap.entrySet())
            {
                String resultDirStr = entry.getKey();
                Map<String, Spec> specsByKeyInOneDirectory = entry.getValue();
                File resultDir = new File(resultDirStr);
                ResultFile resultFile = new ResultFile(new File(resultDir + "/" + getSpecFilename() + ".lb"));
                resultFiles.add(resultFile);
                if (resultDir.exists())
                {
                    if (!resultDir.isDirectory())
                    {
                        throw new RuntimeException("Destination dir "+resultDirStr+" is not a directory");
                    }
                    FileInputStream fis = null;
                    LineNumberReader lis = null;
                    int lineNum = 0;
                    if (resultFile.file.exists())
                    {
                        if (!resultFile.file.isFile())
                        {
                            throw new RuntimeException("Result file "+resultFile.file.getAbsolutePath()+" is not a file!");
                        }
                        int alreadyHere = 0;
                        try
                        {
                            fis = new FileInputStream(resultFile.file);
                            lis = new LineNumberReader(new InputStreamReader(fis));
                            String line = lis.readLine();
                            StringBuilder sb = new StringBuilder(32);
                            while(line != null)
                            {
                                lineNum++;
                                line = line.trim();
                                if (!line.startsWith("#") && !line.isEmpty())
                                {
                                    Spec resultSpec = Spec.parseResult(resultDirStr, resultFile.file, line, sb, lineNum);
                                    if (resultSpec != null)
                                    {
                                        Spec targetSpec = specsByKeyInOneDirectory.get(resultSpec.getUnversionedArtifactKey());
                                        if (targetSpec == null)
                                        {
                                            resultFile.mustRewrite = true;
                                            resultSpec.deleteFile();
                                            debug("removed old file "+resultSpec.getFile().getAbsolutePath());
                                        }
                                        else
                                        {
                                            alreadyHere += targetSpec.compareExisting(resultFile, resultSpec, this);
                                        }
                                    }
                                }
                                line = lis.readLine();
                            }
                        }
                        catch (IOException e)
                        {
                            warn("error reading result file "+resultFile.file.getAbsolutePath());
                        }
                        finally
                        {
                            closeNoException(fis);
                        }
                        if (alreadyHere != specsByKeyInOneDirectory.size())
                        {
                            downloadNecessary(specsByKeyInOneDirectory, resultFile);
                        }
                        else if (resultFile.mustRewrite)
                        {
                            resultFile.rewrite(specsByKeyInOneDirectory);
                        }
                    }
                    else
                    {
                        downloadAll(specsByKeyInOneDirectory, resultFile);
                    }
                }
                else
                {
                    if (!resultDir.mkdirs())
                    {
                        throw new RuntimeException("could not create directory "+resultDir.getAbsolutePath());
                    }
                    downloadAll(specsByKeyInOneDirectory, resultFile);
                }
            }
        }

        private String getSpecFilename()
        {
            int slashIndex = Math.max(this.spec.lastIndexOf('/'), this.spec.lastIndexOf('\\'));
            String result = this.spec;
            if (slashIndex >= 0)
            {
                result = result.substring(slashIndex+1, result.length());
            }
            return result;
        }


        private void downloadAll(Map<String, Spec> specsByKeyInOneDirectory, ResultFile resultFile)
        {
            if (specsByKeyInOneDirectory.size() > 0)
            {
                debug("Queuing all files in "+resultFile.file.getName()+" for download");
                needsWork = true;
                resultFile.mustRewrite = true;
                for (Spec targetSpec : specsByKeyInOneDirectory.values())
                {
                    targetSpec.resultFile = resultFile;
                    queueTargetSpec(targetSpec);
                }
            }
        }

        private void queueTargetSpec(Spec targetSpec)
        {
            targetSpec.resultFile.incrementDownload();
            if (targetSpec.checksum == null)
            {
                this.specToDo = SPEC_TODO_REWRITE;
            }
            try
            {
                specQueue.put(targetSpec);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("unexpected interrupt", e);
            }
        }

        private void downloadNecessary(Map<String, Spec> specsByKeyInOneDirectory, ResultFile resultFile)
        {
            int count = 0;
            for(Spec targetSpec: specsByKeyInOneDirectory.values())
            {
                if (targetSpec.todo == TODO_DOWNLOAD)
                {
                    targetSpec.resultFile = resultFile;
                    count++;
                }
            }
            if (count > 0)
            {
                needsWork = true;
                for(Spec targetSpec: specsByKeyInOneDirectory.values())
                {
                    if (targetSpec.todo == TODO_NOTHING)
                    {
                        resultFile.writeOneSpec(targetSpec);
                    }
                }
            }
            for(Spec targetSpec: specsByKeyInOneDirectory.values())
            {
                if (targetSpec.todo == TODO_DOWNLOAD)
                {
                    queueTargetSpec(targetSpec);
                }
            }
        }

        private Map<String, Map<String, Spec>> organizeSpecs()
        {
            Map<String, Map<String, Spec>> dirToSpecMap = new HashMap<String, Map<String, Spec>>();
            for(int i=0;i<specs.size();i++)
            {
                Spec someSpec = specs.get(i);
                Map<String, Spec> specsForDir = dirToSpecMap.get(someSpec.destDir);
                if (specsForDir == null)
                {
                    specsForDir = new HashMap<String, Spec>();
                    dirToSpecMap.put(someSpec.destDir, specsForDir);
                }
                if (specsForDir.put(someSpec.getUnversionedArtifactKey(), someSpec) != null)
                {
                    throw new RuntimeException("Duplicate artifact id/extension " + someSpec.artifactId + " in spec file " + this.spec+" for destination dir "+someSpec.destDir);
                }
            }
            return dirToSpecMap;
        }

        private void readSpecFile()
        {
            Spec.readSpecFile(this.spec, specs);
            info("read spec file "+this.spec+" with "+specs.size()+" entries");
        }

        private class DownloadThread extends Thread
        {
            public DownloadThread(int threadNum)
            {
                super("Download Thread - " + threadNum);
                this.setDaemon(true);
            }


            @Override
            public void run()
            {
                Spec specToDownload = specQueue.poll();
                while(specToDownload != null)
                {
                    Repo fromRepo = repos.get(specToDownload.repo);
                    if (fromRepo == null)
                    {
                        errors.add("Could not find repo "+specToDownload.repo+" for file "+specToDownload.getFilename());
                    }
                    else if (!checkFileSystem(specToDownload))
                    {
                        try
                        {
                            FileWithSha fileWithSha = Libboot.download(fromRepo.getUrl(specToDownload), specToDownload.destDir, specToDownload.getFilename());
                            if (specToDownload.checksum != null && !fileWithSha.sha.equals(specToDownload.checksum))
                            {
                                errors.add("Downloaded checksum is "+fileWithSha.sha+" which doesn't match expected checksum of "+specToDownload.checksum+" for file "+specToDownload.getFilename());
                            }
                            else
                            {
                                specToDownload.checksum = fileWithSha.sha;
                                specToDownload.resultFile.writeOneSpec(specToDownload);
                            }
                        }
                        catch (Throwable e)
                        {
                            errors.add("Could not download "+specToDownload.getFilename()+" because "+printableError(e));
                        }
                    }
                    else
                    {
                        specToDownload.resultFile.writeOneSpec(specToDownload);
                    }
                    specToDownload.resultFile.decrementDownload();
                    specToDownload = specQueue.poll();
                }
            }

            private boolean checkFileSystem(Spec specToDownload)
            {
                if (specToDownload.checksum == null) return false;
                File existing = new File(specToDownload.destDir, specToDownload.getFilename());
                if (existing.exists() && existing.isFile())
                {
                    InputStream in = null;
                    try
                    {
                        MessageDigest digest;
                        digest = MessageDigest.getInstance("SHA");
                        in = new BufferedInputStream(new FileInputStream(existing));
                        byte[] b = new byte[4096];
                        int count;
                        while ((count = in.read(b)) >= 0)
                        {
                            if (count > 0)
                            {
                                digest.update(b, 0, count);
                            }
                        }
                        if (!bytesToHex(digest.digest()).equals(specToDownload.checksum))
                        {
                            debug("File "+specToDownload.getFilename()+" doesn't match checksum. Deleting.");
                            existing.delete();
                        }
                        else
                        {
                            debug("File "+specToDownload.getFilename()+" matches sha. No download necessary.");
                            return true;
                        }
                    }
                    catch (Exception e)
                    {
                        errors.add("Could not compute sha for " + specToDownload.getFilename()+" because "+printableError(e));
                    }
                    finally
                    {
                        closeNoException(in);
                    }
                }
                return false;
            }
        }

    }

    private static String printableError(Throwable e)
    {
        String result = "";

        while( e != null)
        {
            result += e.getClass().getSimpleName() + ": " + e.getMessage()+"\n";
            StackTraceElement[] stackTrace = e.getStackTrace();
            StackTraceElement traceElement = stackTrace[0];
            result += "@ " + traceElement.getClassName() + "." + traceElement.getMethodName() + " (" + traceElement.getLineNumber() + ")";
            for (int i = 1; i < stackTrace.length; i++)
            {
                traceElement = stackTrace[i];
                result += "\n";
                result += traceElement.getClassName() + "." + traceElement.getMethodName() + " (" + traceElement.getLineNumber() + ")";
            }
            result += "\n";
            e = e.getCause();
        }
        return result;
    }


    private static class ResultFile
    {
        private File file;
        private OutputStream fos;
        private boolean mustRewrite;
        private AtomicInteger downloadCount = new AtomicInteger();

        public ResultFile(File file)
        {
            this.file = file;
        }

        public void incrementDownload()
        {
            downloadCount.incrementAndGet();
        }

        public synchronized void writeOneSpec(Spec spec)
        {
            try
            {
                if (fos == null)
                {
                    fos = new BufferedOutputStream(new FileOutputStream(file), 8096);
                }
                spec.writeResult(fos);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not write to result file "+file.getAbsolutePath(), e);
            }
        }

        public void decrementDownload()
        {
            int left = downloadCount.decrementAndGet();
            if (left == 0)
            {
                close();
            }
        }

        public synchronized void close()
        {
            if (fos != null)
            {
                closeNoException(fos);
                fos = null;
            }
        }

        public void rewrite(Map<String, Spec> specsByKeyInOneDirectory)
        {
            for(Spec s: specsByKeyInOneDirectory.values()) writeOneSpec(s);
            close();
        }
    }

    private static class Spec
    {
        private String repo;
        private String groupId;
        private String artifactId;
        private String version;
        private String extension;
        private String destDir;
        private String checksum;
        private int lineNum;

        private boolean good = true;
        private String filename;
        public int todo = TODO_DOWNLOAD;
        public ResultFile resultFile;

        // <repo>, <groupId>, <artifactId>, <version>, <extension>, <destDir>, <checksum>
        // it's safe to assume groupId and artifactId don't contain comma "[A-Za-z0-9_\\-.]+"
        // comma is forbidden in a version number because it denotes multiple versions in maven
        static Spec parseSpec(String line, StringBuilder sb, int lineNum)
        {
            int count = 0;
            sb.setLength(0);
            Spec result = new Spec();
            result.lineNum = lineNum;
            for(int i=0;i<line.length();i++)
            {
                char c = line.charAt(i);
                if (c == ',')
                {
                    result.setSpecValue(count++, sb.toString().trim(), lineNum);
                    sb.setLength(0);
                }
                else
                {
                    sb.append(c);
                }
            }
            if (sb.length() > 0)
            {
                result.setSpecValue(count++, sb.toString().trim(), lineNum);
            }
            return result;
        }

        public static void writeSpecFile(String specFileName, List<Spec> specs)
        {
            File specFile = new File(specFileName);
            File outSpecFile = new File(specFileName+".lbtmp");
            int lineNum = 0;
            FileInputStream fis = null;
            FileOutputStream fos = null;
            LineNumberReader lis = null;
            try
            {
                fis = new FileInputStream(specFile);
                fos = new FileOutputStream(outSpecFile);
                lis = new LineNumberReader(new InputStreamReader(fis));
                String line = lis.readLine();
                int specIndex = 0;
                int nextLineToOverwrite = specs.get(specIndex).lineNum;
                while(line != null)
                {
                    lineNum++;
                    if (lineNum == nextLineToOverwrite)
                    {
                        specs.get(specIndex).writeSpec(fos);
                        specIndex++;
                        if (specIndex < specs.size())
                        {
                            nextLineToOverwrite = specs.get(specIndex).lineNum;
                        }
                        else
                        {
                            nextLineToOverwrite = -1;
                        }
                    }
                    else
                    {
                        fos.write(line.getBytes());
                    }
                    fos.write('\n');
                    line = lis.readLine();
                }
            }
            catch(FileNotFoundException e)
            {
                throw new RuntimeException("Could not open spec file. File "+ specFileName +" was not found", e);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not write spec file around line "+lineNum, e);
            }
            finally
            {
                closeNoException(fis);
                closeNoException(fos);
            }
            if (!specFile.delete())
            {
                throw new RuntimeException("Could not delete spec file "+specFile.getAbsolutePath());
            }
            if (!outSpecFile.renameTo(specFile))
            {
                throw new RuntimeException("Could not rename temp spec file "+outSpecFile.getAbsolutePath());
            }
        }

        private void writeSpec(FileOutputStream fos) throws IOException
        {
            fos.write(this.repo.getBytes());
            fos.write(',');
            fos.write(this.groupId.getBytes());
            fos.write(',');
            fos.write(this.artifactId.getBytes());
            fos.write(',');
            fos.write(this.version.getBytes());
            fos.write(',');
            fos.write(this.extension.getBytes());
            fos.write(',');
            fos.write(this.destDir.getBytes());
            fos.write(',');
            fos.write(this.checksum.getBytes());
        }

        public static void readSpecFile(String specFileName, List<Spec> parsedSpecs)
        {
            File specFile = new File(specFileName);
            int lineNum = 0;
            FileInputStream fis = null;
            LineNumberReader lis = null;
            try
            {
                fis = new FileInputStream(specFile);
                lis = new LineNumberReader(new InputStreamReader(fis));
                String line = lis.readLine();
                StringBuilder sb = new StringBuilder(32);
                while(line != null)
                {
                    lineNum++;
                    line = line.trim();
                    if (!line.startsWith("#") && !line.isEmpty())
                    {
                        parsedSpecs.add(Spec.parseSpec(line, sb, lineNum));
                    }
                    line = lis.readLine();
                }
            }
            catch(FileNotFoundException e)
            {
                throw new RuntimeException("Could not open spec file. File "+ specFileName +" was not found", e);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not read spec file around line "+lineNum, e);
            }
            finally
            {
                closeNoException(fis);
            }
        }

        private void setSpecValue(int index, String s, int lineNum)
        {
            if (index > 6)
            {
                throw new RuntimeException("Too many things ("+s+") on line "+lineNum+". was expecting <repo>, <groupId>, <artifactId>, <version>, <extension>, <destDir>, <checksum>");
            }
            if (index < 6 && s.isEmpty())
            {
                throw new RuntimeException("On line "+lineNum+" position "+index+" (position is counted by the commas), value must not be blank.");
            }
            switch(index)
            {
                case 0:
                    this.repo = s;
                    return;
                case 1:
                    this.groupId = s;
                    return;
                case 2:
                    this.artifactId = s;
                    return;
                case 3:
                    this.version = s;
                    return;
                case 4:
                    this.extension = s;
                    return;
                case 5:
                    this.destDir = s;
                    return;
                case 6:
                    this.checksum = s;
            }
        }


        public void writeResult(OutputStream fos) throws IOException
        {
            fos.write(this.artifactId.getBytes(UTF8));
            fos.write(',');
            fos.write(this.version.getBytes(UTF8));
            fos.write(',');
            fos.write(this.extension.getBytes(UTF8));
            fos.write(',');
            fos.write(this.checksum.getBytes(UTF8));
            fos.write('\n');
        }

        //<artifactId>, <version>, <extension>, <checksum>
        static Spec parseResult(String resultDirStr, File resultFile, String line, StringBuilder sb, int lineNum)
        {
            int count = 0;
            sb.setLength(0);
            Spec result = new Spec();
            result.destDir = resultDirStr;
            for(int i=0;i<line.length();i++)
            {
                char c = line.charAt(i);
                if (c == ',')
                {
                    result.setResultValue(count++, sb.toString().trim(), lineNum, resultFile);
                    sb.setLength(0);
                }
                else
                {
                    sb.append(c);
                }
            }
            if (sb.length() > 0)
            {
                result.setResultValue(count++, sb.toString().trim(), lineNum, resultFile);
            }
            return result.good ? result : null;
        }

        private void setResultValue(int index, String s, int lineNum, File resultFile)
        {
            if (index > 3)
            {
                good = false;
                warn("messed up line in result file on line "+lineNum+" in file "+resultFile.getAbsolutePath());
            }
            if (index < 4 && s.isEmpty())
            {
                good = false;
                warn("messed up line in result file on line "+lineNum+" in position "+index+" in file "+resultFile.getAbsolutePath());
            }
            switch(index)
            {
                case 0:
                    this.artifactId = s;
                    return;
                case 1:
                    this.version = s;
                    return;
                case 2:
                    this.extension = s;
                    return;
                case 3:
                    this.checksum = s;
            }
        }

        public String getFilename()
        {
            if (filename == null)
            {
                String dot = ".";
                if (this.extension.contains("."))
                {
                    dot = "";
                }
                filename = this.artifactId+"-"+this.version+dot+this.extension;
            }
            return filename;
        }

        public void deleteFile()
        {
            getFile().delete();
        }

        private File getFile()
        {
            return new File(destDir + File.separator + getFilename());
        }

        public boolean fileExists()
        {
            return getFile().exists();
        }

        public int compareWith(Spec result)
        {
            //artifact id and extension are the same
            if (this.version.equals(result.version) && result.checksum != null)
            {
                if (this.checksum == null || this.checksum.equals(result.checksum))
                {
                    return ALL_GOOD;
                }
            }
            return NEEDS_REDOWNLOAD;
        }

        public int compareExisting(ResultFile resultFile, Spec resultSpec, Download download)
        {
            int result = 0;
            this.resultFile = resultFile;
            int delta = this.compareWith(resultSpec);
            if (delta == ALL_GOOD)
            {
                result++;
                if (!resultSpec.fileExists())
                {
                    result--;
                    debug("File "+resultSpec.getFilename()+" does not exist. Will re-download.");
                }
                else
                {
                    debug("File "+resultSpec.getFilename()+" all good.");
                    this.todo = TODO_NOTHING;
                    if (this.checksum == null)
                    {
                        this.checksum = resultSpec.checksum;
                        download.specToDo = SPEC_TODO_REWRITE;
                    }
                }
            }
            else if (delta == NEEDS_REDOWNLOAD)
            {
                resultSpec.deleteFile();
                debug("Removed old file "+resultSpec.getFilename()+". Queuing "+this.getFilename()+" for download.");
                resultFile.mustRewrite = true;
            }
            return result;
        }

        public String getUnversionedArtifactKey()
        {
            return this.artifactId+" "+extension;
        }
    }

    private static class Repo
    {
        private static final String ARTIFACT_ID = "artifactId";
        private static final String VERSION = "version";
        private static final String EXT = "ext";
        private static final String DOT_UNLESS_EXTENSION_DOTTED = "dotUnlessExtensionDotted";
        private static final String GROUP_ID_WITH_SLASHES = "groupIdWithSlashes";
        private static final HashMap<String, String> SUBS = new HashMap<String, String>();
        // urlPattern must include {artifactId} {version} {ext} in it. Most repos will also have {groupIdWithSlashes}
        private String repoName;
        private String urlPattern;
        private List<String> patternLayout = new ArrayList();

        static
        {
            SUBS.put(ARTIFACT_ID, ARTIFACT_ID);
            SUBS.put(VERSION, VERSION);
            SUBS.put(EXT, EXT);
            SUBS.put(DOT_UNLESS_EXTENSION_DOTTED, DOT_UNLESS_EXTENSION_DOTTED);
            SUBS.put(GROUP_ID_WITH_SLASHES, GROUP_ID_WITH_SLASHES);
        }

        public static Repo parseRepo(String line, StringBuilder sb, int lineNum)
        {
            int count = 0;
            sb.setLength(0);
            Repo result = new Repo();
            for (int i = 0; i < line.length(); i++)
            {
                char c = line.charAt(i);
                if (c == ',')
                {
                    result.setValue(count++, sb.toString().trim(), lineNum);
                    sb.setLength(0);
                }
                else
                {
                    sb.append(c);
                }
            }
            if (sb.length() > 0)
            {
                result.setValue(count++, sb.toString().trim(), lineNum);
            }
            result.validateUrlPattern();
            return result;
        }

        private void validateUrlPattern()
        {
            StringBuilder buffer = new StringBuilder(urlPattern.length());
            for(int i=0;i<urlPattern.length();i++)
            {
                char c = urlPattern.charAt(i);
                if (c == '{' && i < urlPattern.length() - 2)
                {
                    int end = urlPattern.indexOf('}', i + 1);
                    if (end > 0)
                    {
                        String inner = urlPattern.substring(i+1, end);
                        String sub = SUBS.get(inner);
                        if (sub != null)
                        {
                            patternLayout.add(buffer.toString());
                            buffer.setLength(0);
                            patternLayout.add(sub);
                            i = end;
                            continue;
                        }
                    }
                }
                buffer.append(c);
            }
            if (buffer.length() > 0)
            {
                patternLayout.add(buffer.toString());
            }
        }

        private void setValue(int index, String s, int lineNum)
        {
            if (index > 2)
            {
                throw new RuntimeException("Too many things ("+s+") on line "+lineNum+". was expecting <repoName>, <urlPattern>");
            }
            switch(index)
            {
                case 0:
                    this.repoName = s;
                    return;
                case 1:
                    this.urlPattern = s;
                    return;
            }
        }

        public String getUrl(Spec specToDownload)
        {
            StringBuilder b = new StringBuilder(urlPattern.length()*2);
            for(int i=0;i<patternLayout.size();i++)
            {
                String part = patternLayout.get(i);
                if (part == ARTIFACT_ID)
                {
                    b.append(specToDownload.artifactId);
                }
                else if (part == VERSION)
                {
                    b.append(specToDownload.version);
                }
                else if (part == DOT_UNLESS_EXTENSION_DOTTED)
                {
                    if (!specToDownload.extension.contains("."))
                    {
                        b.append('.');
                    }
                }
                else if (part == EXT)
                {
                    b.append(specToDownload.extension);
                }
                else if (part == GROUP_ID_WITH_SLASHES)
                {
                    b.append(specToDownload.groupId.replace('.', '/'));
                }
                else
                {
                    b.append(part);
                }
            }
            return b.toString();
        }
    }

    public static FileWithSha download(String fileUrl, String dirToDownloadTo, String downloadFileName) throws IOException
    {
        debug("Downloading "+downloadFileName+" from "+fileUrl);
        FileOutputStream out = null;
        InputStream in = null;
        URLConnection conn = null;
        File file = new File(dirToDownloadTo, downloadFileName+".lbtmp");
        try
        {
            MessageDigest digest;
            digest = MessageDigest.getInstance("SHA");
            URL url = new URL(fileUrl);
            conn = handleRedirect(url.openConnection(), 5);
            in = conn.getInputStream();
            out = new FileOutputStream(file);
            byte[] b = new byte[4096];
            int count;
            while ((count = in.read(b)) >= 0)
            {
                if (count > 0)
                {
                    digest.update(b, 0, count);
                }
                out.write(b, 0, count);
            }
            out.flush();
            closeNoException(out);
            out = null;
            File realFile = new File(dirToDownloadTo, downloadFileName);
            if (realFile.exists() && !realFile.delete())
            {
                throw new RuntimeException("Could not delete old file "+realFile.getAbsolutePath());
            }
            if (!file.renameTo(realFile))
            {
                throw new RuntimeException("Could not rename to "+dirToDownloadTo+"/"+downloadFileName);
            }
            file = null;
            return new FileWithSha(realFile, bytesToHex(digest.digest()));
        }
        catch (IOException e)
        {
            throw enhanceExceptionIfHttpConnection(fileUrl, e, conn);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not download or compute sha for "+fileUrl, e);
        }
        finally
        {
            closeNoException(out);
            closeNoException(in);
            if (file != null)
            {
                file.delete();
            }
        }
    }

    private static URLConnection handleRedirect(URLConnection urlConnection, int retriesLeft) throws IOException
    {
        if (retriesLeft == 0)
        {
            throw new IOException("too many redirects connecting to "+urlConnection.getURL().toString());
        }
        if (urlConnection instanceof HttpURLConnection)
        {
            HttpURLConnection conn = (HttpURLConnection) urlConnection;

            int status = conn.getResponseCode();
            if (status == HttpURLConnection.HTTP_MOVED_PERM || status == HttpURLConnection.HTTP_MOVED_TEMP || status == HttpURLConnection.HTTP_SEE_OTHER)
            {
                String newUrl = conn.getHeaderField("Location");
                return handleRedirect(new URL(newUrl).openConnection(), retriesLeft - 1);
            }
        }
        return urlConnection;
    }

    private static class FileWithSha
    {
        private File file;
        private String sha;

        public FileWithSha(File file, String sha)
        {
            this.file = file;
            this.sha = sha;
        }
    }

    private static IOException enhanceExceptionIfHttpConnection(String fileUrl, IOException originalException, URLConnection conn)
    {
        StringBuilder exceptionDetails = new StringBuilder();
        exceptionDetails.append("Unable to download ").append(fileUrl).append("\n");

        int responseCode = -1;
        if (conn instanceof HttpURLConnection)
        {
            try
            {
                HttpURLConnection httpURLConnection = (HttpURLConnection) conn;
                responseCode = httpURLConnection.getResponseCode();
                exceptionDetails.append("HTTP code "+responseCode);
                if (responseCode == 400)
                {
                    exceptionDetails.append("Resource not found on server.\n");
                }
                exceptionDetails.append("Server returned response code ").append(responseCode).append("\n");
                if (responseCode == 500)
                {
                    InputStream errorStream = httpURLConnection.getErrorStream();
                    if (errorStream != null)
                    {
                        try
                        {
                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(errorStream));
                            String nextLine = null;
                            while ((nextLine = bufferedReader.readLine()) != null)
                            {
                                exceptionDetails.append(nextLine).append("\n");
                            }
                        }
                        finally
                        {
                            errorStream.close();
                        }
                    }
                }
            }
            catch (IOException e)
            {
                //ignored
            }
        }
        return new IOException(exceptionDetails.toString() + "\ncaused by "+originalException.getClass().getSimpleName()+": "+originalException.getMessage(), originalException);
    }

    public static String createSha(File file) throws FileNotFoundException
    {
        FileInputStream fileInputStream = null;
        try
        {
            fileInputStream = new FileInputStream(file);
            return createSha(fileInputStream);
        }
        finally
        {
            closeNoException(fileInputStream);
        }

    }

    public static String createSha(String fileUrl)
    {
        InputStream in = null;
        try
        {
            in = new URL(fileUrl).openConnection().getInputStream();
            return createSha(in);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeNoException(in);
        }
    }

    private static String createSha(InputStream inputStream)
    {
        try
        {
            MessageDigest digest;
            digest = MessageDigest.getInstance("SHA");
            byte[] buffer = new byte[1024 * 8];
            int numRead;

            do
            {
                numRead = inputStream.read(buffer);
                if (numRead > 0)
                {
                    digest.update(buffer, 0, numRead);
                }
            }
            while (numRead != -1);
            return bytesToHex(digest.digest());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }


    private static String bytesToHex(byte[] bytes)
    {
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for (int j = 0; j < bytes.length; j++)
        {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}

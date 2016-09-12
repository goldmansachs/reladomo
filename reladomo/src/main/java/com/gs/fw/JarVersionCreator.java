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

package com.gs.fw;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Jar;
import org.apache.tools.ant.types.ZipFileSet;
import org.apache.tools.zip.ZipOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;


public class JarVersionCreator extends Jar
{
    private String applicationName;
    private String version;
    private CrcInputStream crcInputStream = new CrcInputStream();
    private List<FileChecksum> fileChecksums = new ArrayList<FileChecksum>(200);

    @Override
    public void execute() throws BuildException
    {
        checkVersionAndName();
        super.execute();
    }

    private void checkVersionAndName()
    {
        checkName();
        if (this.version == null || version.trim().length() == 0)
        {
            throw new BuildException("version cannot be null or blank");
        }
    }

    @Override
    protected void zipFile(InputStream is, ZipOutputStream zOut, String vPath, long lastModified, File fromArchive, int mode) throws IOException
    {
        if (vPath.startsWith("META-INF/"))
        {
            if (!vPath.equals("META-INF/"+applicationName+".crcver"))
            {
                super.zipFile(is, zOut, vPath, lastModified, fromArchive, mode);
            }
        }
        else
        {
            crcInputStream.resetStream(is);
            super.zipFile(crcInputStream, zOut, vPath, lastModified, fromArchive, mode);
            fileChecksums.add(new FileChecksum(vPath, crcInputStream.getCrcValue()));
        }

    }

    @Override
    protected void finalizeZipOutputStream(ZipOutputStream zOut) throws IOException, BuildException
    {
        createVersionInfo(zOut);
        super.finalizeZipOutputStream(zOut);
    }

    private void createVersionInfo(ZipOutputStream zOut) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(baos, "UTF8"));

        CRC32 fullCrc = new CRC32();
        // header
        writer.print("name: ");
        writer.println(applicationName);
        writer.print("version: ");
        writer.println(this.version);

        fullCrc.update(applicationName.getBytes("UTF8"));
        fullCrc.update(version.getBytes("UTF8"));

        writeVersionInfo(writer, fullCrc);
        writer.println();
        writer.print(":crc: ");
        writer.println(Long.toHexString(fullCrc.getValue()));
        
        if (writer.checkError())
        {
            throw new IOException("Encountered an error writing jar version information");
        }
        writer.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        super.zipFile(bais, zOut, "META-INF/"+applicationName+".crcver", System.currentTimeMillis(), null, ZipFileSet.DEFAULT_FILE_MODE);
        bais.close();
    }

    private void writeVersionInfo(PrintWriter writer, CRC32 fullCrc) throws IOException
    {
        if (fileChecksums.size() == 0) return;
        Collections.sort(fileChecksums);
        String currentPackage = null;
        for(int i=0;i<fileChecksums.size();i++)
        {
            String vPath = fileChecksums.get(i).vPath;
            fullCrc.update(vPath.getBytes("UTF8"));
            currentPackage = writePackage(writer, currentPackage, vPath);
            String filename = vPath.substring(currentPackage.length());
            writer.print(filename);
            writer.print(": ");
            String crc = Long.toHexString(fileChecksums.get(i).crc);
            writer.println(crc);
            fullCrc.update(crc.getBytes("UTF8"));
        }
    }

    private String writePackage(PrintWriter writer, String currentPackage, String vPath)
    {
        String newPackage = getDir(vPath);
        if (currentPackage == null || !currentPackage.equals(newPackage))
        {
            writer.print("/");
            writer.println(newPackage);
        }
        return newPackage;
    }

    private String getDir(String vPath)
    {
        int index = vPath.lastIndexOf('/');
        if (index < 0) return "";
        return vPath.substring(0, index + 1);
    }

    @Override
    protected void cleanUp()
    {
        super.cleanUp();
        fileChecksums.clear();
    }

    public void setApplicationName(String applicationName)
    {
        this.applicationName = applicationName;
    }

    public void setAppName(String applicationName)
    {
        this.applicationName = applicationName;
    }

    private void checkName()
    {
        if (applicationName == null || applicationName.trim().length() == 0)
        {
            throw new BuildException("application name cannot be null");
        }
        if (!applicationName.toLowerCase().equals(applicationName))
        {
            throw new BuildException("application name must be all lowercase");
        }
        if (applicationName.contains(" "))
        {
            throw new BuildException("application name must not have spaces");
        }
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    private static class CrcInputStream extends InputStream
    {
        private InputStream in;
        private CRC32 crc32 = new CRC32();

        @Override
        public int read() throws IOException
        {
            int result = this.in.read();
            if (result != -1)
            {
                crc32.update(result);
            }
            return result;
        }

        @Override
        public int read(byte[] b) throws IOException
        {
            int result = in.read(b);
            if (result > 0)
            {
                crc32.update(b, 0, result);
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            int result = in.read(b, off, len);
            if (result > 0)
            {
                crc32.update(b, off, result);
            }
            return result;
        }

        @Override
        public int available() throws IOException
        {
            return in.available();
        }

        @Override
        public void close() throws IOException
        {
            in.close();
        }

        public void resetStream(InputStream in)
        {
            this.in = in;
            this.crc32.reset();
        }

        public long getCrcValue()
        {
            return crc32.getValue();
        }
    }

    private static class FileChecksum implements Comparable
    {
        private String vPath;
        private long crc;

        private FileChecksum(String vPath, long crc)
        {
            this.vPath = vPath;
            this.crc = crc;
        }

        public int compareTo(Object o)
        {
            return this.vPath.compareTo(((FileChecksum)o).vPath);
        }
    }
}

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

package com.gs.fw.common.mithra.generator;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;


public class MithraGeneratorImport
{
    private String filename;
    private String dir;
    private String archive;
    private String[] excludeList;
    private FileProvider fileProvider;

    public void init()
    {
    }

    public String getFilename()
    {
        return filename;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }

    public void setDir(String dir)
    {
        this.dir = dir;
    }

    public void setArchive(String archive)
    {
        this.archive = archive;
    }

    public String[] getExcludeList()
    {
        return excludeList;
    }

    public void setExcludeList(String excludeListString)
    {
        excludeList = excludeListString.split(",");
    }

    public FileProvider getFileProvider()
    {
        if (fileProvider == null)
        {
            if (archive != null)
            {
                fileProvider = new JarFileProvider(excludeList, archive, dir);
            }
            else if (dir != null)
            {
                fileProvider = new DirectoryFileProvider(excludeList, dir);
            }
            else
            {
                throw new MithraGeneratorException("Cannot determine file provider to use - both archive and dir are not set.");
            }
        }
        return fileProvider;
    }

    static interface FileProvider
    {
        public FileInputStreamWithSize getFileInputStream(String fileName) throws FileNotFoundException;
        public String getSourceName();
        public boolean excludeObject(String objectName);
        public void close();
    }

    public static class FileInputStreamWithSize
    {
        private InputStream inputStream;
        private long size;

        public FileInputStreamWithSize(InputStream inputStream, long size)
        {
            this.inputStream = inputStream;
            this.size = size;
        }

        public InputStream getInputStream()
        {
            return inputStream;
        }

        public long getSize()
        {
            return size;
        }
    }

    static abstract class AbstractFileProvider implements FileProvider
    {
        private List excludeList;

        public AbstractFileProvider(String[] excludeList)
        {
            this.excludeList = excludeList == null ? null : Arrays.asList(excludeList);
        }

        public boolean excludeObject(String objectName)
        {
            return excludeList != null && excludeList.contains(objectName);
        }
    }

    public static class DirectoryFileProvider extends AbstractFileProvider
    {
        private String dir;

        public DirectoryFileProvider(String dir)
        {
            super(null);
            this.dir = dir;
        }

        public DirectoryFileProvider(String[] excludeList, String dir)
        {
            super(excludeList);
            this.dir = dir;
        }

        public FileInputStreamWithSize getFileInputStream(String fileName) throws FileNotFoundException
        {
            File file = new File(this.dir, fileName);
            return new FileInputStreamWithSize(new FileInputStream(file), file.length());
        }

        public String getSourceName()
        {
            return dir;
        }

        public void close()
        {
        }
    }

    static class JarFileProvider extends AbstractFileProvider
    {
        private String dir;
        private JarFile jarFile;

        public JarFileProvider(String[] excludeList, String archive, String dir)
        {
            super(excludeList);

            try
            {
                this.jarFile = new JarFile(archive);
                this.dir = dir;
            }
            catch (IOException e)
            {
                throw new MithraGeneratorException("unable to find mithra import archive '" + archive + "'", e);
            }
        }

        public FileInputStreamWithSize getFileInputStream(String fileName) throws FileNotFoundException
        {
            String fullFileName = this.getDirectoryPrefix() + fileName;
            try
            {
                ZipEntry entry = jarFile.getEntry(fullFileName);
                if(entry == null) throw new MithraGeneratorException("unable to find '" + fullFileName + "' in import archive file '" + jarFile.getName() + "'");
                return new FileInputStreamWithSize(jarFile.getInputStream(entry), entry.getSize());
            }
            catch (IOException e)
            {
                close();
                throw new MithraGeneratorException("unexpected error while reading '" + fullFileName + "' from import archive '" + jarFile.getName() + "'", e);
            }

        }

        private String getDirectoryPrefix()
        {
            // nb, "/" is required for  JAR files
            return dir == null ? "" : (dir + "/");
        }

        public void close()
        {
            try
            {
                jarFile.close();
            }
            catch (IOException e)
            {
                throw new MithraGeneratorException("can't close archive jar file '" + jarFile.getName() + "'", e);
            }
        }

        public String getSourceName()
        {
            return jarFile.getName();
        }
    }

}

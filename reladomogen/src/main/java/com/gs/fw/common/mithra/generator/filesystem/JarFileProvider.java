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

package com.gs.fw.common.mithra.generator.filesystem;

import com.gs.fw.common.mithra.generator.MithraGeneratorException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public class JarFileProvider extends AbstractFileProvider
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
        // nb, "/" is required for  JAR files, not File.separator
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

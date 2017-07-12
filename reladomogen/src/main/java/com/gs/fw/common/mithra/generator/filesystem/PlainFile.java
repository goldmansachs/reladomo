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

import java.io.*;

public class PlainFile implements FauxFile
{
    private File file;

    public PlainFile(String path)
    {
        this.file = new File(path);
    }

    public PlainFile(PlainFile parent, String path)
    {
        this.file = new File(parent.file, path);
    }

    @Override
    public boolean exists()
    {
        return this.file.exists();
    }

    @Override
    public String getName()
    {
        return this.file.getName();
    }

    @Override
    public boolean mkdirs()
    {
        return this.file.mkdirs();
    }

    @Override
    public long length()
    {
        return this.file.length();
    }

    @Override
    public boolean canWrite()
    {
        return this.file.canWrite();
    }

    @Override
    public boolean isDirectory()
    {
        return this.file.isDirectory();
    }

    @Override
    public FileOutputStream newFileOutputStream() throws IOException
    {
        return new FileOutputStream(this.file);
    }

    @Override
    public FileInputStream newFileInputStream() throws FileNotFoundException
    {
        return new FileInputStream(this.file);
    }

    @Override
    public String getParent()
    {
        return this.file.getParent();
    }

    @Override
    public String getPath()
    {
        return this.file.getPath();
    }
}

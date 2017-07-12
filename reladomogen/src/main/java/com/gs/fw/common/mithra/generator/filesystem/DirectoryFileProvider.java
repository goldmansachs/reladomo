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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class DirectoryFileProvider extends AbstractFileProvider
{
    private String dir;
    private FauxFileSystem fauxFileSystem;

    public DirectoryFileProvider(FauxFileSystem fauxFileSystem, String dir)
    {
        super(null);
        this.dir = dir;
        this.fauxFileSystem = fauxFileSystem;
    }

    public DirectoryFileProvider(FauxFileSystem fauxFileSystem, String[] excludeList, String dir)
    {
        super(excludeList);
        this.dir = dir;
        this.fauxFileSystem = fauxFileSystem;
    }

    public FileInputStreamWithSize getFileInputStream(String fileName) throws FileNotFoundException
    {
        FauxFile file = this.fauxFileSystem.newFile(this.dir, fileName);
        return new FileInputStreamWithSize(file.newFileInputStream(), file.length());
    }

    public String getSourceName()
    {
        return dir;
    }

    public void close()
    {
    }
}

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

import com.gs.fw.common.mithra.generator.filesystem.*;


public class MithraGeneratorImport
{
    private String filename;
    private String dir;
    private String archive;
    private String[] excludeList;
    private FileProvider fileProvider;
    private FauxFileSystem fauxFileSystem;

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
                fileProvider = new DirectoryFileProvider(this.fauxFileSystem, excludeList, dir);
            }
            else
            {
                throw new MithraGeneratorException("Cannot determine file provider to use - both archive and dir are not set.");
            }
        }
        return fileProvider;
    }

    public void setFauxFileSystem(FauxFileSystem fauxFileSystem)
    {
        this.fauxFileSystem = fauxFileSystem;
    }

    public FauxFileSystem getFauxFileSystem()
    {
        return fauxFileSystem;
    }
}

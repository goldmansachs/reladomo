
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

package com.gs.fw.common.mithra.bulkloader;

import com.gs.fw.common.mithra.util.TableColumnInfo;

import java.io.File;
import java.io.IOException;

public class SybaseIqBcpFile extends SybaseBcpFile
{

    public SybaseIqBcpFile(TableColumnInfo tableMetadata, String[] columns, String loadDirectory)
            throws IOException
    {
        super(tableMetadata, columns, loadDirectory);
    }

    protected File createDataFile(String temporaryFilePrefix, String loadDirectory)
            throws IOException
    {
        return new File(loadDirectory, temporaryFilePrefix+".bcp");
    }
}

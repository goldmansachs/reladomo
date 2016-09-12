
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

package com.gs.fw.common.mithra.test.bulkloader;

import com.gs.fw.common.mithra.bulkloader.SybaseBcpFile;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import junit.framework.TestCase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Types;


public class SybaseBcpFileTest extends TestCase
{

    public void testBcpFile() throws Exception
    {
        ColumnInfo[] columns = new ColumnInfo[3];
        columns[0] = new ColumnInfo("OBJECTID", Types.INTEGER, 18, 0, 0, 1, false);
        columns[1] = new ColumnInfo("USERID", Types.VARCHAR, 32, 0, 0, 2, true);
        columns[2] = new ColumnInfo("NAME", Types.VARCHAR, 64, 0, 0, 3, false);

        TableColumnInfo tableColumnInfo = new TableColumnInfo("", "dbo", "user", columns);

        SybaseBcpFile bcpFile = new SybaseBcpFile(tableColumnInfo, new String[] {"OBJECTID", "NAME"});
        try
        {
            // Bind a couple of rows and check the data file for correctness
            bcpFile.writeColumn("1");
            bcpFile.writeColumn("Fred");
            bcpFile.endRow();
            bcpFile.writeColumn("2");
            bcpFile.writeColumn("Jane");
            bcpFile.endRow();
            bcpFile.writeColumn("3");
            bcpFile.writeColumn("Harry");
            bcpFile.endRow();

            bcpFile.close();

            assertFileContents(
                "Wrong data file contents.",
                "1\tFred\n"
                + "2\tJane\n"
                + "3\tHarry\n",
                bcpFile.getDataFile()
            );
        }
        finally
        {
            bcpFile.delete();
        }
    }

    private void assertFileContents(String errorMessage, String expectedFileContents, File file) throws IOException
    {
        StringBuffer fileContents = new StringBuffer();

        FileReader reader = new FileReader(file);
        try
        {
            char[] buffer = new char[128];
            int length = -1;

            while ((length = reader.read(buffer)) != -1)
            {
                fileContents.append(buffer, 0, length);
            }
        }
        finally
        {
            reader.close();
        }

        assertEquals(errorMessage, expectedFileContents, fileContents.toString());
    }
}

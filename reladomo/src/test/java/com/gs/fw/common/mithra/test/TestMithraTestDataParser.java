
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.test.domain.AccountTransactionMax;
import com.gs.fw.common.mithra.test.domain.AllTypes;
import com.gs.fw.common.mithra.test.domain.AllTypesData;
import com.gs.fw.common.mithra.test.domain.Player;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.fileparser.BinaryCompressor;
import com.gs.fw.common.mithra.util.fileparser.MithraParsedData;
import junit.framework.TestCase;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.zip.ZipInputStream;

public class TestMithraTestDataParser
extends TestCase
{
    public void testSimpleDataParsing()
    {
        String testFile = "testdata/testMithraTestDataParser.txt";
        MithraTestDataParser parser = new MithraTestDataParser(testFile);
        List list = parser.getResults();

        assertNotNull("No results returned", list);
        assertEquals(25, list.size());

        // AccountTransactionMax
        MithraParsedData atm = (MithraParsedData) list.get(0);
        assertTrue(atm.getParsedClassName().equals(AccountTransactionMax.class.getName()));
        assertEquals(3, atm.getDataObjects().size());
        assertEquals(3, atm.getAttributes().size());

        // Player
        MithraParsedData player = (MithraParsedData) list.get(23);
        assertTrue(player.getParsedClassName().equals(Player.class.getName()));
        assertEquals(4, player.getDataObjects().size());
        assertEquals(5, player.getAttributes().size());

        MithraParsedData allTypes = (MithraParsedData) list.get(24);
        assertTrue(allTypes.getParsedClassName().equals(AllTypes.class.getName()));
        List<MithraDataObject> dataObjects = allTypes.getDataObjects();
        assertTrue(dataObjects.get(0) instanceof AllTypesData);
        AllTypesData allTypesData = (AllTypesData) dataObjects.get(0);
        assertEquals(51242172543926290L, allTypesData.getLongValue());

    }

    public void testSimpleZippedDataParsing() throws Exception
    {
        String testFile = "testdata/testMithraTestDataParser.zip";
        URL testFileLocation = this.getClass().getClassLoader().getResource(testFile).toURI().toURL();
        ZipInputStream zipInputStream = new ZipInputStream(this.getClass().getClassLoader().getResourceAsStream(testFile));

        zipInputStream.getNextEntry();  // advance to the first file in the zip.
        MithraTestDataParser parser = new MithraTestDataParser(testFileLocation, zipInputStream);
        List list = parser.getResults();

        assertNotNull("No results returned", list);
        assertEquals(24, list.size());

        // AccountTransactionMax
        MithraParsedData atm = (MithraParsedData) list.get(0);
        assertTrue(atm.getParsedClassName().equals(AccountTransactionMax.class.getName()));
        assertEquals(3, atm.getDataObjects().size());
        assertEquals(3, atm.getAttributes().size());

        // Player
        MithraParsedData player = (MithraParsedData) list.get(23);
        assertTrue(player.getParsedClassName().equals(Player.class.getName()));
        assertEquals(4, player.getDataObjects().size());
        assertEquals(5, player.getAttributes().size());
    }

    public void testColumnarBinaryFormat() throws Exception
    {
        String filename = "testdata/mithraTestDataDefaultSource.txt";
        MithraTestDataParser parser = new MithraTestDataParser(filename);
        List<MithraParsedData> data = parser.getResults();
        BinaryCompressor binaryCompressor = new BinaryCompressor();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(70 * 1024);
        binaryCompressor.compressData(data, baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        MithraFastList<MithraParsedData> newData = binaryCompressor.decompress(new URL("file://in-memory"), bais);
        compareData(data, newData);
    }

    //todo: fix long parsing of large longs

    public static void compareData(List<MithraParsedData> data, List<MithraParsedData> newData)
    {
        assertEquals(data.size(), newData.size());
        for(int i=0;i<data.size();i++)
        {
            compareData(data.get(i), newData.get(i));
        }
    }

    private static void compareData(MithraParsedData oldData, MithraParsedData newData)
    {
        assertEquals(oldData.getParsedClassName(), newData.getParsedClassName());
        List<MithraDataObject> oldObjects = oldData.getDataObjects();
        List<MithraDataObject> newObjects = newData.getDataObjects();
        assertEquals(oldObjects.size(), newObjects.size());
        for(int i=0;i<oldObjects.size();i++)
        {
            MithraDataObject old = oldObjects.get(i);
            MithraDataObject neu = newObjects.get(i);
            assertTrue(old.hasSamePrimaryKeyIgnoringAsOfAttributes(neu));
            assertTrue(old.zHasSameNullPrimaryKeyAttributes(neu));
            if (old.changed(neu))
            {
                System.out.println("break me");
            }
            assertFalse(old.changed(neu));
        }
    }
}

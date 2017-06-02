
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

package com.gs.fw.common.mithra.test.generator;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.generator.objectxmlgenerator.ForeignKey;
import com.gs.fw.common.mithra.generator.objectxmlgenerator.TableInfo;
import junit.framework.Assert;
import junit.framework.TestCase;

public class TableInfoTest
        extends TestCase
{
    public void testRemoveForeignKeysForMissingTables()
    {
        TableInfo tableInfo = new TableInfo();
        tableInfo.getForeignKeys().put("Z", new ForeignKey(null, null, "A"));
        tableInfo.removeForeignKeysForMissingTables(UnifiedSet.newSetWith("A", "K"));
        Assert.assertEquals(1, tableInfo.getForeignKeys().size());
        tableInfo.removeForeignKeysForMissingTables(UnifiedSet.newSetWith("Z", "K"));
        Assert.assertEquals(0, tableInfo.getForeignKeys().size());
    }

    public void testGetClassName()
    {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName("TEST_TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test_table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test_table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());

        tableInfo.setTableName("TEST__TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test__table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test__Table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test__table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test__TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());

        tableInfo.setTableName("TEST___TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test___table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test___Table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test___table");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test___TABLE");
        Assert.assertEquals("TestTable", tableInfo.getClassName());

        tableInfo.setTableName("TEST_TABLE1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("test_table1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("Test_table1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());

        tableInfo.setTableName("TEST_TABLE_I");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test_table_I");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table_I");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("Test_table_I");
        Assert.assertEquals("TestTable", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE_I");
        Assert.assertEquals("TestTable", tableInfo.getClassName());

        tableInfo.setTableName("TEST_TABLE_II");
        Assert.assertEquals("TestTableIi", tableInfo.getClassName());
        tableInfo.setTableName("test_table_II");
        Assert.assertEquals("TestTableIi", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table_II");
        Assert.assertEquals("TestTableIi", tableInfo.getClassName());
        tableInfo.setTableName("Test_table_II");
        Assert.assertEquals("TestTableIi", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE_II");
        Assert.assertEquals("TestTableIi", tableInfo.getClassName());

        tableInfo.setTableName("TEST_TABLE_11");
        Assert.assertEquals("TestTable11", tableInfo.getClassName());
        tableInfo.setTableName("test_table_11");
        Assert.assertEquals("TestTable11", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table_11");
        Assert.assertEquals("TestTable11", tableInfo.getClassName());
        tableInfo.setTableName("Test_table_11");
        Assert.assertEquals("TestTable11", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE_11");
        Assert.assertEquals("TestTable11", tableInfo.getClassName());

        tableInfo.setTableName("TEST_TABLE_1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("test_table_1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("Test_Table_1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("Test_table_1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());
        tableInfo.setTableName("test_TABLE_1");
        Assert.assertEquals("TestTable1", tableInfo.getClassName());

    }
}

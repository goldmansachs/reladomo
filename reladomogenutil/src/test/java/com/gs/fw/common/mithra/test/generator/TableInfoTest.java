
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
}
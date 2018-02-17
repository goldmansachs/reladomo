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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test.cacheloader;

import com.gs.fw.common.mithra.cacheloader.CacheLoaderEngine;
import com.gs.fw.common.mithra.cacheloader.DependentSingleKeyIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;


public class DependentSingleKeyIndexTest extends TestCase
{
    public void testInsert()
    {
        CacheLoaderEngine engine = new MockCacheLoaderEngine();
        DependentSingleKeyIndex index = new DependentSingleKeyIndex(engine,
                new MockTaskSpawner(engine), new Extractor[0]);
        int totalAdded = 0;
        int totalRemoved = 0;
        for (int k = 0; k < 100; k++)
        {
            List list = FastList.newList();
            for (int i = 0; i < 100 * k; i++)
            {
                String s = k + ":" + i;
                list.add(s);
                totalAdded++;
            }
            index.addStripe(list);

            totalRemoved += index.createListForTaskRunner().size();
            assertEquals(index.size(), totalAdded - totalRemoved);
        }

        int removed;
        while ((removed = index.createListForTaskRunner().size()) > 0)
        {
            totalRemoved += removed;
            assertEquals(index.size(), totalAdded - totalRemoved);
        }
        assertEquals(totalAdded, totalRemoved);
        assertEquals(index.size(), totalAdded - totalRemoved);
    }

}

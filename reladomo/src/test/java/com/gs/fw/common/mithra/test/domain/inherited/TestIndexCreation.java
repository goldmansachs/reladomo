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
package com.gs.fw.common.mithra.test.domain.inherited;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullNonDatedCache;
import com.gs.fw.common.mithra.cache.Index;

public class TestIndexCreation extends TestCase
{
    public void testLewPosition()
    {
        TestCache cache = new TestCache();
        TxMonkeyFinder.initializeIndicies(cache);
        Index[] indices = cache.getIndices();
        assertEquals(2, indices.length);
        assertTrue(indices[0].isUnique());
        assertEquals(1, indices[1].getExtractors().length);
        assertEquals("animalGroupId", ((Attribute) indices[1].getExtractors()[0]).getAttributeName());
    }

    private static class TestCache extends FullNonDatedCache
    {
        private TestCache()
        {
            super(new Attribute[0], null);
        }

        @Override
        protected Index[] getIndices()
        {
            return super.getIndices();
        }
    }
}

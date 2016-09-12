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

import com.gs.fw.common.mithra.cache.NonUniqueIdentityIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import junit.framework.TestCase;

import java.sql.Timestamp;


public class TestNonUniqueIdentityIndex extends TestCase
{
    private final static int N = 17;

    public void testSize()
    {
        Extractor[] extractors = {new TestExtractor()};
        NonUniqueIdentityIndex index = new NonUniqueIdentityIndex(extractors, 7);
        int count = 0;
        int k = 0;
        for (k=0; k<13; k++)
        {
            for (int i = 0; i < N; i++)
            {
                TestObject value = new TestObject();
                value.nonUniqueKey = i;
                value.identity = i;
                index.put(value);
                count++;

                assertEquals(count, index.size());
            }

            assertEquals(k+1, index.getAverageReturnSize());
        }
        for (; k<5; k--)
        {
            for (int i = 0; i < N; i++)
            {
                TestObject value = new TestObject();
                value.nonUniqueKey = i;
                value.identity = i;
                index.remove(value);
                count--;

                assertEquals(count, index.size());
            }
    
            assertEquals(k+1, index.getAverageReturnSize());
        }
    }

    private class TestObject
    {
        int nonUniqueKey;
        int identity;

        public boolean equals(Object o)
        {
            return this.identity == ((TestObject) o).identity;
        }

        public int hashCode()
        {
            return identity;
        }
    }

    private static class TestExtractor implements IntExtractor
    {
        public int intValueOf(Object o)
        {
            return ((TestObject) o).nonUniqueKey;
        }

        public void setIntValue(Object o, int newValue)
        {
            ((TestObject) o).nonUniqueKey = newValue;
        }

        public void setValue(Object o, Object newValue)
        {
        }

        public void setValueNull(Object o)
        {
        }

        public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
        {
        }

        public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
        {
        }

        public boolean isAttributeNull(Object o)
        {
            return false;
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            return ((TestObject) first).nonUniqueKey == ((TestObject) second).nonUniqueKey;
        }

        public int valueHashCode(Object object)
        {
            return ((TestObject) object).nonUniqueKey;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return ((TestObject) first).nonUniqueKey == ((TestObject) second).nonUniqueKey;
        }

        public Object valueOf(Object object)
        {
            return ((TestObject) object).nonUniqueKey;
        }
    }
}

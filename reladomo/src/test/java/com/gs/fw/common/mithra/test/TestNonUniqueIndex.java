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

import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.NonUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.util.DoUntilProcedure2;
import junit.framework.TestCase;

import java.sql.Timestamp;


public class TestNonUniqueIndex extends TestCase
{
    private static final int MAX = 10000;

    public void testPutAndGet()
    {
        runTestPutAndGet(0);
        runTestPutAndGet(1);
        runTestPutAndGet(2);
        runTestPutAndGet(3);
    }

    public void runTestPutAndGet (int shift)
    {
        Extractor[] nExtractors = {new ShiftedNExtractor(-shift)};
        Extractor[] uExtractors = {new ShiftedUExtractor(shift)};
        NonUniqueIndex index = new NonUniqueIndex("Pushkin", uExtractors, nExtractors, 7);
        for (int i=0; i<MAX; i++)
        {
            TestObject value = new TestObject(i%157, i);
            index.put(value);
        }
        for (int i=0; i<MAX; i++)
        {
            TestObject key = new TestObject(i%17, i);
            Object values = index.get(key, nExtractors);
            if (values instanceof FullUniqueIndex)
            {
                FullUniqueIndex set = (FullUniqueIndex)values;
                assertTrue(set.size() > 1);
                assertTrue(set.size() < 157);

                set.forAllWith(new DoUntilProcedure2()
                {
                    public boolean execute(Object object, Object key)
                    {
                        assertEquals(((TestObject)object).n, ((TestObject)key).n);
                        return false;
                    }
                }, key);
            }
            else
            {
                assertEquals(key.u, ((TestObject)values).u);
            }

            assertTrue(index.getAverageReturnSize() > MAX / 200);
            assertTrue(index.getAverageReturnSize() < 200);
            assertTrue(index.contains(key, nExtractors, null));
        }

    }

    private class TestObject
    {
        int n;
        int u;

        private TestObject(int n, int u)
        {
            this.n = n;
            this.u = u;
        }
    }

    private static class ShiftedNExtractor implements IntExtractor
    {
        private int shift;

        private ShiftedNExtractor(int shift)
        {
            this.shift = shift;
        }

        public int intValueOf(Object o)
        {
            return ((TestObject)o).n;
        }

        public void setIntValue(Object o, int newValue)
        {
            ((TestObject)o).n = newValue;
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
            return ((TestObject)first).n == ((TestObject)second).n;
        }

        public int valueHashCode(Object object)
        {
            return ((TestObject)object).n >> shift;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return ((TestObject)first).n == ((TestObject)second).n;
        }

        public Object valueOf(Object object)
        {
            return ((TestObject)object).n;
        }
    }
    private static class ShiftedUExtractor implements IntExtractor
    {
        private int shift;

        private ShiftedUExtractor(int shift)
        {
            this.shift = shift;
        }

        public int intValueOf(Object o)
        {
            return ((TestObject)o).u;
        }

        public void setIntValue(Object o, int newValue)
        {
            ((TestObject)o).u = newValue;
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
            return ((TestObject)first).u == ((TestObject)second).u;
        }

        public int valueHashCode(Object object)
        {
            return ((TestObject)object).u >> shift;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return ((TestObject)first).u == ((TestObject)second).u;
        }

        public Object valueOf(Object object)
        {
            return ((TestObject)object).u;
        }
    }
}

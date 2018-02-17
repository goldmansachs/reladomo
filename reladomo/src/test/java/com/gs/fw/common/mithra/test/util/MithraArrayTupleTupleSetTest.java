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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.util.List;

public class MithraArrayTupleTupleSetTest extends TestCase
{
    public void testsetOfIntIntTuple () throws Exception
    {
        Extractor[] extractors = new Extractor[2];
        extractors[0] = new TestIntExtractor(0);
        extractors[1] = new TestIntExtractor(1);

        List dataHolders = FastList.newList();
        int[] dataHolder1 = {1, 10};
        dataHolders.add(dataHolder1);
        int[] dataHolder2 = {2, 20};
        dataHolders.add(dataHolder2);
        int[] dataHolder3 = {3, 10};
        dataHolders.add(dataHolder3);
        int[] dataHolder4 = {1, 20};
        dataHolders.add(dataHolder4);
        int[] dataHolder5 = {1, 10};
        dataHolders.add(dataHolder5);
        MithraArrayTupleTupleSet set = new MithraArrayTupleTupleSet (extractors, dataHolders);

        assertEquals(4, set.size());
        assertTrue(set.contains(dataHolder1, extractors));
        assertTrue(set.contains(dataHolder5, extractors));

        int[] dataHolder6 = {3, 30};
        assertFalse(set.contains(dataHolder6, extractors));

        MithraArrayTupleTupleSet set2 = SerializationTestUtil.serializeDeserialize(set);

        assertEquals(4, set2.size());
        assertTrue(set2.contains(dataHolder1, extractors));

        set.addAll(extractors, FastList.newListWith(dataHolder6));
        assertTrue(set.contains(dataHolder6, extractors));
    }

    private static class TestIntExtractor implements IntExtractor
    {
        private int index;

        public TestIntExtractor (int index)
        {
            this.index = index;
        }

        public int intValueOf(Object o)
        {
            return ((int[]) o)[index];
        }

        public void setIntValue(Object o, int newValue)
        {
            ((int[]) o)[index] = newValue;
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
            return this.valueEquals(first, second);
        }

        public int valueHashCode(Object object)
        {
            return ((int[]) object)[index];
        }

        public boolean valueEquals(Object first, Object second)
        {
            return ((int[]) first)[index] == ((int[]) second)[index];
        }

        public Object valueOf(Object object)
        {
            return Integer.valueOf(intValueOf(object)); // MithraTupleTupleSet checks that both values are Integers.
        }
    }

}

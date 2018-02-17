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

import com.gs.fw.common.mithra.util.ConstantIntSet;
import com.gs.fw.common.mithra.util.ConstantShortSet;
import com.gs.fw.common.mithra.util.ConstantStringSet;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.IOException;

public class ConstantSetsTest extends TestCase
{
    public void testConstantIntSetSerialization() throws IOException, ClassNotFoundException
    {
        ConstantIntSet emptySet = new ConstantIntSet(new int[] {});
        ConstantIntSet nonEmptySet = new ConstantIntSet(new int[] {-1, 0, 42});

        ConstantIntSet emptyCopy = SerializationTestUtil.serializeDeserialize(emptySet);
        ConstantIntSet nonEmptyCopy = SerializationTestUtil.serializeDeserialize(nonEmptySet);

        assertEquals(emptySet, emptyCopy);
        assertEquals(nonEmptySet, nonEmptyCopy);
    }

    public void testConstantShortSetSerialization() throws IOException, ClassNotFoundException
    {
        ConstantShortSet emptySet = new ConstantShortSet(new short[] {});
        ConstantShortSet nonEmptySet = new ConstantShortSet(new short[] {-1, 0, 42});

        ConstantShortSet emptyCopy = SerializationTestUtil.serializeDeserialize(emptySet);
        ConstantShortSet nonEmptyCopy = SerializationTestUtil.serializeDeserialize(nonEmptySet);

        assertEquals(emptySet, emptyCopy);
        assertEquals(nonEmptySet, nonEmptyCopy);
    }

    public void testConstantStringSetSerialization() throws IOException, ClassNotFoundException
    {
        ConstantStringSet emptySet = new ConstantStringSet(FastList.<String>newListWith());
        ConstantStringSet nonEmptySet = new ConstantStringSet(FastList.newListWith("Fred", "Jim", "Sheila"));

        ConstantStringSet emptyCopy = SerializationTestUtil.serializeDeserialize(emptySet);
        ConstantStringSet nonEmptyCopy = SerializationTestUtil.serializeDeserialize(nonEmptySet);

        assertEquals(emptySet, emptyCopy);
        assertEquals(nonEmptySet, nonEmptyCopy);
    }
}

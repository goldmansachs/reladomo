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


import com.gs.fw.common.mithra.cache.ConcurrentOffHeapStringIndex;
import com.gs.fw.common.mithra.cache.ConcurrentOnHeapStringIndex;
import com.gs.fw.common.mithra.cache.StringIndex;

public class TestConcurrentOffHeapStringIndex extends TestConcurrentStringIndex
{
    @Override
    protected StringIndex createStringPool()
    {
        return new ConcurrentOffHeapStringIndex(10);
    }

    public void testHardening()
    {
        StringIndex index = createStringPool();

        String testString = "abcd";

        for(int i=0;i<10;i++)
        {
            testString = testString + i; // hopefully, the compiler won't intern this
        }

        String pooled = index.getIfAbsentPut(testString, false);
        assertEquals(pooled, testString);
        assertSame(pooled, index.getIfAbsentPut(testString, false));

        index.getIfAbsentPutOffHeap(testString);

        assertSame(pooled, index.getIfAbsentPut(testString, false));

        int hardIndex = index.getOffHeapReference(testString);

        assertTrue(hardIndex != StringIndex.NULL_STRING && hardIndex != StringIndex.UNKNOWN_STRING);

        String one = "AaAa";
        String two = "BBBB";
        String three = "AaBB";
        String four = "BBAa";

        assertEquals(one.hashCode(), two.hashCode());
        assertEquals(one.hashCode(), three.hashCode());
        assertEquals(one.hashCode(), four.hashCode());

        String onePooled = index.getIfAbsentPut(one, false);
        String twoPooled = index.getIfAbsentPut(two, false);
        String threePooled = index.getIfAbsentPut(three, false);
        String fourPooled = index.getIfAbsentPut(four, false);

        index.getIfAbsentPutOffHeap(two);
        index.getIfAbsentPutOffHeap(four);

        assertEquals(StringIndex.UNKNOWN_STRING, index.getOffHeapReference(one));
        assertEquals(StringIndex.UNKNOWN_STRING, index.getOffHeapReference(three));
        hardIndex = index.getOffHeapReference(two);
        assertTrue(hardIndex != StringIndex.NULL_STRING && hardIndex != StringIndex.UNKNOWN_STRING);
        hardIndex = index.getOffHeapReference(four);
        assertTrue(hardIndex != StringIndex.NULL_STRING && hardIndex != StringIndex.UNKNOWN_STRING);
    }
}

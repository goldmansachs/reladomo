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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.NonUniqueIndex;
import com.gs.fw.common.mithra.connectionmanager.LruListWithThreadAffinity;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.util.CollectionUtil;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.MithraCompositeList;
import com.gs.fw.common.mithra.util.MithraFastList;
import junit.framework.TestCase;
import org.eclipse.collections.impl.block.factory.Comparators;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Arrays;
import java.util.Random;


public class TestCollections extends TestCase
{
    private static long populateArrays(Integer[] list, Integer[] copy)
    {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        for (int i = 0; i < list.length; i++)
        {
            list[i] = ((int) (rand.nextDouble() * 100000)); // (int)(Math.random() * 1000000)
            copy[i] = list[i];
        }
        return seed;
    }

    public void testRandomUniqueArray()
    {
        int max = 10000;
        Integer[] list = new Integer[max];
        Integer[] copy = new Integer[max];
        long seed = 0;
        for(int i=0;i<20;i++)
        {
            seed = populateArrays(list, copy);
            CollectionUtil.psort(list, CollectionUtil.COMPARABLE_COMPARATOR);
            Arrays.sort(copy);
        }
        assertTrue("sort failed with seed "+seed,Arrays.equals(copy, list));
    }

    public void testRandomNonUniqueArray()
    {
        int max = 300000;
        Integer[] list = new Integer[max];
        Integer[] copy = new Integer[max];
        long seed = 0;
        for(int i=0;i<2;i++)
        {
            seed = populateArrays(list, copy);
            CollectionUtil.psort(list, CollectionUtil.COMPARABLE_COMPARATOR);
            Arrays.sort(copy);
        }
        assertTrue("sort failed with seed "+seed,Arrays.equals(copy, list));
    }

    public void testInternalListAdd()
    {
        InternalList list = new InternalList();
        for(int i=0;i<10000;i++)
        {
            list.add(i);
        }
        assertEquals(10000, list.size());
        for(int i=0;i<10000;i++)
        {
            assertEquals(i, ((Integer)list.get(i)).intValue());
        }
    }

    public void testInternalListSort()
    {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        InternalList list = new InternalList();
        for (int i = 0; i < 50000; i++)
        {
            list.add((int) (rand.nextDouble() * 100000));
        }
        list.sort();
        for(int i=0; i< list.size() - 1 ; i++)
        {
            assertTrue(((Integer) list.get(i)) <= ((Integer) list.get(i + 1)));
        }

        list.sort(Comparators.reverseNaturalOrder());
        for(int i=0; i< list.size() - 1 ; i++)
        {
            assertTrue(((Integer) list.get(i)) >= ((Integer) list.get(i + 1)));
        }

        InternalList list2 = new InternalList();
        for (int i = 0; i < 7; i++)
        {
            list2.add((int) (rand.nextDouble()) * 100000);
        }
        list2.sort();
        for(int i=0; i< list2.size() - 1 ; i++)
        {
            assertTrue(((Integer) list2.get(i)) <= ((Integer) list2.get(i + 1)));
        }
        list2.sort(Comparators.reverseNaturalOrder());
        for(int i=0; i< list2.size() - 1 ; i++)
        {
            assertTrue(((Integer) list2.get(i)) >= ((Integer) list2.get(i + 1)));
        }
    }

    public void testInteralListAddAt()
    {
        InternalList list = new InternalList();
        for(int i=0;i<10000;i+=2)
        {
            list.add(i);
        }
        for(int i=1;i<10000;i+=2)
        {
            list.add(i,i);
        }
        assertEquals(10000, list.size());
        for(int i=0;i<10000;i++)
        {
            assertEquals(i, ((Integer)list.get(i)).intValue());
        }
    }

    public void testInteralListAddAllAt()
    {
        InternalList toAdd = new InternalList();
        toAdd.add(-5);
        toAdd.add(-10);
        InternalList list = new InternalList();
        for(int i=0;i<10000;i+=3)
        {
            list.add(i);
        }
        for(int i=1;i<10000;i+=3)
        {
            list.addAll(i, toAdd);
        }
        assertEquals(10000, list.size());
        for(int i=0;i<10000-2;i+=3)
        {
            assertEquals(i, ((Integer)list.get(i)).intValue());
            assertEquals(-5, ((Integer)list.get(i+1)).intValue());
            assertEquals(-10, ((Integer)list.get(i+2)).intValue());
        }
    }

    public void testRemoveByReplacingFromEnd()
    {
        InternalList list = new InternalList();
        for(int i=0;i<10000;i++)
        {
            list.add(i);
        }
        for(int i=0;i<10000;i++)
        {
            list.removeByReplacingFromEnd(0);
            assertEquals(10000 - i - 1, list.size());
        }
    }

    public void testInternalListRemove()
    {
        InternalList list = new InternalList();
        for(int i=0;i<10000;i++)
        {
            list.add(i);
            list.add(-50);
        }
        for(int i=1;i<=10000;i++)
        {
            list.remove(i);
        }
        assertEquals(10000, list.size());
        for(int i=0;i<10000;i++)
        {
            assertEquals(i, ((Integer)list.get(i)).intValue());
        }
    }

    public void testLruListAddRemove()
    {
        LruListWithThreadAffinity list = new LruListWithThreadAffinity();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<30000;i++)
        {
            list.add(i);
            assertEquals(i+1, list.size());
            set.add(i);
        }
        for(int i=0;i<30000;i++)
        {
            set.remove((Integer)list.remove());
            assertEquals(30000 - i - 1, list.size());
        }
        assertEquals(0, set.size());
    }

    public void testLruListThreadAffinity() throws InterruptedException
    {
        final LruListWithThreadAffinity list = new LruListWithThreadAffinity();
        final boolean[] success = new boolean[2];
        list.add(10);
        list.add(20);

        Thread t1 = new Thread() {
            public void run()
            {
                Object first;
                synchronized (list)
                {
                    first = list.remove();
                    list.add(first);
                }

                for(int i=0;i<100000;i++)
                {
                    synchronized (list)
                    {
                        if (first != list.remove())
                        {
                            success[0] = false;
                        }
                    }
                    synchronized (list)
                    {
                        list.add(first);
                    }
                }
                success[0] = true;
            }
        };

        Thread t2 = new Thread() {
            public void run()
            {
                Object first;
                synchronized (list)
                {
                    first = list.remove();
                    list.add(first);
                }

                for(int i=0;i<100000;i++)
                {
                    synchronized (list)
                    {
                        if (first != list.remove())
                        {
                            success[1] = false;
                        }
                    }
                    synchronized (list)
                    {
                        list.add(first);
                    }
                }
                success[1] = true;
            }
        };

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertTrue(success[0]);
        assertTrue(success[1]);
    }

    public void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while(now < target)
        {
            try
            {
                Thread.sleep(target-now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
    }

    public void testLruListRemoveIdle()
    {
        long now = System.currentTimeMillis();
        final LruListWithThreadAffinity list = new LruListWithThreadAffinity();
        for(int i=0;i<10;i++)
        {
            list.add(i);
        }
        assertEquals(0, list.removeEvictable(now - 100, 10000).size());
        sleep(10);
        assertEquals(10, list.removeEvictable(System.currentTimeMillis() - 9, 11).size());
        assertEquals(0, list.size());

    }

    public void testRebalance()
    {
        MithraFastList<Integer>[] lists = new MithraFastList[24];
        int pos = 0;
        int totalSize = 1500000;
        for(int i=0;i<24;i++)
        {
            int size = i* 10000;
            lists[i] = new MithraFastList(size);
            for(; size > 0 && pos < totalSize; pos++, size--)
            {
                lists[i].add(pos);
            }
        }
        MithraCompositeList compositeList = new MithraCompositeList(lists);
        assertEquals(totalSize, compositeList.size());
        FastList<MithraFastList<Integer>> balancedLists = compositeList.getLists();
        assertEquals(totalSize, compositeList.size());
        int maxIdealSize = (int) (totalSize*1.1/24);
        for(int i=0;i<balancedLists.size();i++)
        {
            assertTrue(balancedLists.get(i).size() <= maxIdealSize);
        }
        UnifiedSet<Integer> integers = new UnifiedSet<Integer>(compositeList);
        assertEquals(totalSize, integers.size());
    }

    public void testNonUniqueIndex()
    {
        NonUniqueIndex index = new NonUniqueIndex("", new Extractor[]{OrderFinder.orderId()}, new Extractor[]{OrderFinder.userId()});
        for(int i=0;i<10;i++)
        {
            index.put(createOrder(i,i));
        }
        assertEquals(10, index.size());
        assertEquals(10, index.getNonUniqueSize());
        for(int i=10000;i<20000;i++)
        {
            index.put(createOrder(i, i >> 1)); // 2 per slot
        }
        assertEquals(10010, index.size());
        assertEquals(5010, index.getNonUniqueSize());
        for(int i=1010000;i<1020000;i++)
        {
            index.put(createOrder(i, i >> 4)); // 16 per slot
        }
        assertEquals(20010, index.size());
        assertEquals(5010+10000/16, index.getNonUniqueSize());
        for(int i=0;i<10;i++)
        {
            assertEquals(i, ((Order)index.get(i)).getOrderId());
        }
        for(int i=10000;i<20000;i++)
        {
            FullUniqueIndex fui = (FullUniqueIndex) index.get(i >> 1);
            assertEquals(2, fui.size());
            assertEquals(i >> 1, ((Order)fui.getFirst()).getUserId());
        }
        for(int i=1010000;i<1020000;i++)
        {
            FullUniqueIndex fui = (FullUniqueIndex) index.get(i >> 4);
            assertEquals(16, fui.size());
            assertEquals(i >> 4, ((Order)fui.getFirst()).getUserId());
        }
        for(int i=0;i<10;i+=2)
        {
            index.remove(createOrder(i,i));
        }
        assertEquals(20005, index.size());
        assertEquals(5000+5+10000/16, index.getNonUniqueSize());
        for(int i=10000;i<20000;i+=2)
        {
            index.remove(createOrder(i, i >> 1)); // 2 per slot
        }
        assertEquals(5+5000+10000, index.size());
        assertEquals(5+5000+10000/16, index.getNonUniqueSize());
        for(int i=1010000;i<1020000;i+=2)
        {
            index.remove(createOrder(i, i >> 4)); // 16 per slot
        }
        assertEquals(5+5000+5000, index.size());
        assertEquals(5+5000+10000/16, index.getNonUniqueSize());
        for(int i=10001;i<15001;i+=2)
        {
            index.remove(createOrder(i, i >> 1)); // 2 per slot
        }
        assertEquals(5+2500+5000, index.size());
        assertEquals(5+2500+10000/16, index.getNonUniqueSize());

    }

    private Order createOrder(int orderId, int userId)
    {
        Order o = new Order();
        o.setOrderId(orderId);
        o.setUserId(userId);
        return o;
    }
}

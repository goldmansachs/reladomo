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

package com.gs.fw.common.mithra.util;

import java.util.*;


public class CollectionUtil
{
    public static final Comparator COMPARABLE_COMPARATOR = new Comparator() {
        public int compare(Object o1, Object o2)
        {
            return ((Comparable) o1).compareTo(o2);
        }
    };

    private static void swap(Object[] a, int first, int second)
    {
        Object tmp = a[first];
        a[first] = a[second];
        a[second] = tmp;
    }

    public static void psort(Object[] a, Comparator cmp)
    {
        psort(a, 0, 1, a.length - 1, cmp);
    }

    public static void psort(Object[] a, int size, Comparator cmp)
    {
        psort(a, 0, 1, size - 1, cmp);
    }

    private static void psort(Object[] a, int offset, int s, int toIndex, Comparator cmp)
    {
        if (s <= 0) s = 1;
        int pu = offset + s;

        if (toIndex < 7)
        {
            for (; s <= toIndex; s++)
            {
                for (int pj = pu; pj > offset && cmp.compare(a[pj - 1], a[pj]) > 0; pj--)
                    swap(a, pj, pj - 1);
                pu++;
            }
            return;
        }

        Arrays.sort(a, 0, toIndex + 1, cmp);
    }
}
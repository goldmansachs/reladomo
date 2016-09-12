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

import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategoryFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategoryList;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.TupleSet;

public class TestTimeTuple extends MithraTestAbstract
{
    public void testTupleSet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(1, Time.withMillis(1, 2, 2, 3));
        set.add(1, Time.withMillis(1, 2, 2, 3));
        set.add(3, Time.withMillis(3, 11, 23, 0));
        set.add(3, Time.withMillis(1, 2, 2, 3));

        TupleAttribute tupleAttribute = AlarmCategoryFinder.category().tupleWith(AlarmCategoryFinder.time());
        AlarmCategoryList list = new AlarmCategoryList(tupleAttribute.in(set));
        list.addOrderBy(AlarmCategoryFinder.id().ascendingOrderBy());
        assertEquals(2, list.size());

        assertEquals("alarm 6", list.get(0).getDescription());
        assertEquals("alarm 600", list.get(1).getDescription());
    }
}

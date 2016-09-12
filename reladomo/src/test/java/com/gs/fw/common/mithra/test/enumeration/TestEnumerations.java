
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

package com.gs.fw.common.mithra.test.enumeration;

import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.enumeration.Day;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class TestEnumerations extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();
        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    public void testOne()
    {
        EnumSet daySet = EnumSet.allOf(Day.class);
        assertTrue(EnumSet.complementOf(daySet).isEmpty());
    }
}

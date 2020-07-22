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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.SortedSet;
import java.util.TreeSet;


public class TestAndOperationToString extends MithraTestAbstract
{
    public void testAndWithMultipleConditions()
    {
        Operation op = ParaDeskFinder.deskIdString().eq("lsd").and(ParaDeskFinder.statusChar().eq('A')).and(ParaDeskFinder.connectionLong().greaterThan(1000000L));
        assertEquals("ParaDesk.statusChar = A & ParaDesk.deskIdString = \"lsd\" & ParaDesk.connectionLong > 1000000", op.toString());
    }

    public void testAndWithConditionsAndIn()
    {
        SortedSet<String> stringSet = new TreeSet<String>(UnifiedSet.newSetWith("lsd", "swp"));
        Operation op = ParaDeskFinder.deskIdString().in(stringSet).and(ParaDeskFinder.statusChar().eq('A')).and(ParaDeskFinder.connectionLong().greaterThan(1000000L));
        assertEquals("ParaDesk.deskIdString in [\"lsd\", \"swp\"] & ParaDesk.statusChar = A & ParaDesk.connectionLong > 1000000", op.toString());
    }

    public void testAndWithDuplicates()
    {
        Operation op = UserFinder.sourceId().eq(0);
        op = op.and(UserFinder.id().greaterThan(1));
        op = op.and(UserFinder.id().greaterThan(1));
        op = op.and(UserFinder.sourceId().eq(0));
        assertEquals("User.sourceId = 0 & User.id > 1", op.toString());
    }

    public void testAndWithOr()
    {
        Operation op1 = UserFinder.sourceId().greaterThan(0);
        op1 = op1.and(UserFinder.id().eq(1));
        Operation op2 = UserFinder.sourceId().eq(0);
        op2 = op2.and(UserFinder.id().eq(2));
        Operation op = op1.or(op2);
        assertEquals(" ( ( User.sourceId > 0 & User.id = 1 ) | ( User.id = 2 & User.sourceId = 0 ) )", op.toString());
    }
}

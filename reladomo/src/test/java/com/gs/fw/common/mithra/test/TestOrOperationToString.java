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
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.sql.Timestamp;
import java.util.SortedSet;
import java.util.TreeSet;


public class TestOrOperationToString extends MithraTestAbstract
{
    public void testOrWithMultipleEquals()
    {
        Operation op = ParaDeskFinder.deskIdString().eq("lsd").or(ParaDeskFinder.statusChar().eq('A'));
        assertEquals(" ( ( ParaDesk.deskIdString = \"lsd\" ) | ( ParaDesk.statusChar = A ) )", op.toString());
    }

    public void testOrWithMultipleIn()
    {
        LongHashSet gscLongSet = LongHashSet.newSetWith(1000000L, 2000000L);
        SortedSet<Timestamp> timestampSet = new TreeSet<Timestamp>(UnifiedSet.newSetWith(InfinityTimestamp.getParaInfinity(), new Timestamp(getDawnOfTime().getTime())));
        Operation op = ParaDeskFinder.connectionLong().in(gscLongSet).or(ParaDeskFinder.createTimestamp().in(timestampSet));
        assertTrue(op.toString(), op.toString().equals(" ( ( ParaDesk.connectionLong in [1000000, 2000000] ) | ( ParaDesk.createTimestamp in [\"1900-01-01 00:00:00.0\", \"9999-12-01 23:59:00.0\"] ) )")
                || op.toString().equals(" ( ( ParaDesk.connectionLong in [2000000, 1000000] ) | ( ParaDesk.createTimestamp in [\"1900-01-01 00:00:00.0\", \"9999-12-01 23:59:00.0\"] ) )"));
    }

    public void testOrWithEqualsAndIn()
    {
        SortedSet<String> stringSet = new TreeSet<String>(UnifiedSet.newSetWith("lsd", "swp"));
        Operation op = ParaDeskFinder.deskIdString().in(stringSet).or(ParaDeskFinder.statusChar().eq('A'));
        assertEquals(" ( ( ParaDesk.deskIdString in [\"lsd\", \"swp\"] ) | ( ParaDesk.statusChar = A ) )", op.toString());
    }

    public void testOrWithDuplicates()
    {
        Operation op = UserFinder.sourceId().eq(0);
        op = op.or(UserFinder.id().eq(1));
        op = op.or(UserFinder.id().eq(1));
        op = op.or(UserFinder.sourceId().eq(0));
        assertEquals(" ( ( User.sourceId = 0 ) | ( User.id = 1 ) | ( User.id = 1 ) | ( User.sourceId = 0 ) )", op.toString());
    }

    public void testOrWithAnd()
    {
        Operation op1 = OrderFinder.items().discountPrice().greaterThan(0).or(OrderFinder.userId().eq(1));
        Operation op2 = OrderFinder.items().discountPrice().lessThan(20).or(OrderFinder.userId().eq(2));
        Operation op = op1.and(op2);
        assertEquals(" ( ( Order.items.discountPrice > 0.0 ) | ( Order.userId = 1 ) ) & ( ( Order.items.discountPrice < 20.0 ) | ( Order.userId = 2 ) )", op.toString());
    }
}

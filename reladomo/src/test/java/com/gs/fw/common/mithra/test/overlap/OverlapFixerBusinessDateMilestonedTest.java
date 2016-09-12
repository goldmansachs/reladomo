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

package com.gs.fw.common.mithra.test.overlap;


import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.test.domain.*;

public class OverlapFixerBusinessDateMilestonedTest extends AbstractOverlapFixerTest
{
    private static final Operation USER_OP = TestOverlapBusinessDateMilestonedFinder.businessDate().equalsEdgePoint().
            and(TestOverlapBusinessDateMilestonedFinder.user().eq("whitba"));

    public OverlapFixerBusinessDateMilestonedTest()
    {
        super(TestOverlapBusinessDateMilestoned.class, USER_OP);
    }

    @Override
    protected MithraList getFixedRowsInExpectedOrder()
    {
        Operation allRowsOp = TestOverlapBusinessDateMilestonedFinder.businessDate().equalsEdgePoint();
        return this.createObjectListInExpectedOrder(allRowsOp);
    }

    protected MithraList getFixedRowsInExpectedOrderWithOperation()
    {
        Operation op = TestOverlapBusinessDateMilestonedFinder.businessDate().equalsEdgePoint().and(USER_OP);
        return this.createObjectListInExpectedOrder(op);
    }

    private TestOverlapBusinessDateMilestonedList createObjectListInExpectedOrder(Operation op)
    {
        TestOverlapBusinessDateMilestonedList fixed = new TestOverlapBusinessDateMilestonedList(op);
        fixed.addOrderBy(TestOverlapBusinessDateMilestonedFinder.overlapId().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapBusinessDateMilestonedFinder.businessDateFrom().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapBusinessDateMilestonedFinder.businessDateTo().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapBusinessDateMilestonedFinder.type().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapBusinessDateMilestonedFinder.user().ascendingOrderBy());
        return fixed;
    }
}
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

public class OverlapFixerFullyMilestonedTest extends AbstractOverlapFixerTest
{
    private static final Operation USER_OP = TestOverlapFullyMilestonedFinder.businessDate().equalsEdgePoint().
            and(TestOverlapFullyMilestonedFinder.processingDate().equalsEdgePoint()).
            and(TestOverlapFullyMilestonedFinder.user().eq("whitba"));

    public OverlapFixerFullyMilestonedTest()
    {
        super(TestOverlapFullyMilestoned.class, USER_OP);
    }

    @Override
    protected MithraList getFixedRowsInExpectedOrder()
    {
        Operation allRowsOp = TestOverlapFullyMilestonedFinder.businessDate().equalsEdgePoint();
        allRowsOp = allRowsOp.and(TestOverlapFullyMilestonedFinder.processingDate().equalsEdgePoint());
        return this.createObjectListInExpectedOrder(allRowsOp);
    }

    @Override
    protected MithraList getFixedRowsInExpectedOrderWithOperation()
    {
        Operation op = TestOverlapFullyMilestonedFinder.businessDate().equalsEdgePoint().and(USER_OP);
        op = op.and(TestOverlapFullyMilestonedFinder.processingDate().equalsEdgePoint());
        return this.createObjectListInExpectedOrder(op);

    }

    private TestOverlapFullyMilestonedList createObjectListInExpectedOrder(Operation op)
    {
        TestOverlapFullyMilestonedList fixed = new TestOverlapFullyMilestonedList(op);
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.overlapId().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.processingDateFrom().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.processingDateTo().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.businessDateFrom().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.businessDateTo().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.type().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapFullyMilestonedFinder.user().ascendingOrderBy());
        return fixed;
    }
}
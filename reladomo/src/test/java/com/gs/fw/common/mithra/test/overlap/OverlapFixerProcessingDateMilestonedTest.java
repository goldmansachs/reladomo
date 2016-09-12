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

public class OverlapFixerProcessingDateMilestonedTest extends AbstractOverlapFixerTest
{
    private static final Operation USER_OP = TestOverlapProcessingDateMilestonedFinder.processingDate().equalsEdgePoint().
            and(TestOverlapProcessingDateMilestonedFinder.user().eq("whitba"));

    public OverlapFixerProcessingDateMilestonedTest()
    {
        super(TestOverlapProcessingDateMilestoned.class, USER_OP);
    }

    @Override
    protected MithraList getFixedRowsInExpectedOrder()
    {
        Operation allRowsOp = TestOverlapProcessingDateMilestonedFinder.processingDate().equalsEdgePoint();
        return this.createObjectListInExpectedOrder(allRowsOp);

    }

    @Override
    protected MithraList getFixedRowsInExpectedOrderWithOperation()
    {
        Operation op = TestOverlapProcessingDateMilestonedFinder.processingDate().equalsEdgePoint().and(USER_OP);;
        return this.createObjectListInExpectedOrder(op);

    }

    private TestOverlapProcessingDateMilestonedList createObjectListInExpectedOrder(Operation op)
    {
        TestOverlapProcessingDateMilestonedList fixed = new TestOverlapProcessingDateMilestonedList(op);
        fixed.addOrderBy(TestOverlapProcessingDateMilestonedFinder.overlapId().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapProcessingDateMilestonedFinder.processingDateFrom().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapProcessingDateMilestonedFinder.processingDateTo().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapProcessingDateMilestonedFinder.type().ascendingOrderBy());
        fixed.addOrderBy(TestOverlapProcessingDateMilestonedFinder.user().ascendingOrderBy());
        return fixed;
    }
}
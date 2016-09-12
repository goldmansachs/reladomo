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


import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import java.sql.Timestamp;

public class TestMixedSqlTimestampJoin extends MithraTestAbstract
{
    @Override
    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        NontemporalWithDateJoin.class,
                        BitemporalWithDateJoin.class,
                        ProcessingTemporalWithDateJoin.class
                };
    }

    public void testJoin()
    {
        Timestamp businessDate = newTimestamp("2012-06-01 00:00:00");

        NontemporalWithDateJoinList list = NontemporalWithDateJoinFinder.findMany(NontemporalWithDateJoinFinder.businessDate().eq(businessDate)
                .and(NontemporalWithDateJoinFinder.bitemporalWithDateJoin().processingTemporalWithDateJoin().approvalStatus().notIn(UnifiedSet.newSetWith("C", "D"))));

        ProcessingTemporalWithDateJoinList testList = list.getBitemporalWithDateJoin().getProcessingTemporalWithDateJoins();
        assertEquals(1, testList.size());
    }

    public void testAdhocDeepFetch()
    {
        Timestamp[] businessDates = new Timestamp[2];
        businessDates[0] = newTimestamp("2012-06-01 00:00:00");
        businessDates[1] = newTimestamp("2012-08-01 00:00:00");

        ProcessingTemporalWithDateJoinList list = new ProcessingTemporalWithDateJoinList();
        for(int i=0;i<10000;i++)
        {
            ProcessingTemporalWithDateJoin temporal = new ProcessingTemporalWithDateJoin(InfinityTimestamp.getParaInfinity());
            temporal.setBusinessDate(businessDates[i % 2]);
            temporal.setActive(1);
            temporal.setInvestmentId(i+10000);
            temporal.setEntity(""+(i+10000));
            list.add(temporal);
        }
        list.bulkInsertAll();

        BitemporalWithDateJoinList bitemporalList = new BitemporalWithDateJoinList();
        for(int i=0;i<10000;i++)
        {
            BitemporalWithDateJoin temporal = new BitemporalWithDateJoin(businessDates[i % 2]);
            temporal.setInvestmentId(i+10000);
            temporal.setGsEntity("" + (i + 10000));
            bitemporalList.add(temporal);
        }
        bitemporalList.bulkInsertAll();
        bitemporalList.clear();

        BitemporalWithDateJoinFinder.clearQueryCache();
        ProcessingTemporalWithDateJoinFinder.clearQueryCache();

        Operation op = ProcessingTemporalWithDateJoinFinder.investmentId().greaterThanEquals(10000);

        ProcessingTemporalWithDateJoinList all = new ProcessingTemporalWithDateJoinList(op);
        all.deepFetch(ProcessingTemporalWithDateJoinFinder.bitemporalWithDateJoin());

        assertEquals(10000, all.size());
        for(int i=0;i<all.size();i++)
        {
            assertNotNull(all.get(i).getBitemporalWithDateJoin());
        }

        BitemporalWithDateJoinFinder.clearQueryCache();
        ProcessingTemporalWithDateJoinFinder.clearQueryCache();

        op = ProcessingTemporalWithDateJoinFinder.investmentId().greaterThanEquals(10000);
        op = op.and(ProcessingTemporalWithDateJoinFinder.investmentId().lessThan(15000));
        op = op.and(ProcessingTemporalWithDateJoinFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() + 10000)));

        ProcessingTemporalWithDateJoinList firstHalf = new ProcessingTemporalWithDateJoinList(op);

        op = ProcessingTemporalWithDateJoinFinder.investmentId().greaterThanEquals(15000);
        op = op.and(ProcessingTemporalWithDateJoinFinder.investmentId().lessThan(20000));
        op = op.and(ProcessingTemporalWithDateJoinFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        ProcessingTemporalWithDateJoinList secondHalf = new ProcessingTemporalWithDateJoinList(op);

        ProcessingTemporalWithDateJoinList adhoc = new ProcessingTemporalWithDateJoinList();
        adhoc.addAll(firstHalf);
        adhoc.addAll(secondHalf);

        adhoc.deepFetch(ProcessingTemporalWithDateJoinFinder.bitemporalWithDateJoin());

        assertEquals(10000, adhoc.size());
        for(int i=0;i<adhoc.size();i++)
        {
            assertNotNull(adhoc.get(i).getBitemporalWithDateJoin());
        }

    }

    public void testDefaultEqualityPropagationFromTimestampToAsOfAttribute()
    {
        Operation operation = ProcessingTemporalWithDateJoinFinder.businessDate().eq(newTimestamp("2012-06-01 00:00:00"));
        operation = operation.and(ProcessingTemporalWithDateJoinFinder.bitemporalWithDateJoin().abcdBitemporalWithDateJoin().exists());
        assertEquals(1, ProcessingTemporalWithDateJoinFinder.findMany(operation).size());
    }

    public void testDefaultEqualityPropagationNonEquality()
    {
        Operation operation = ProcessingTemporalWithDateJoinFinder.businessDate().greaterThanEquals(newTimestamp("2012-06-01 00:00:00"));
        operation = operation.and(ProcessingTemporalWithDateJoinFinder.bitemporalWithDateJoin().abcdBitemporalWithDateJoin().exists());
        assertEquals(1, ProcessingTemporalWithDateJoinFinder.findMany(operation).size());
    }
}
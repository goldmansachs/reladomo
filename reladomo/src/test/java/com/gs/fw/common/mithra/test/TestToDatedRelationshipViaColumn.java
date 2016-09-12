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

//import com.gs.fw.common.mithra.AggregateList;
//import com.gs.fw.common.mithra.finder.Operation;
//import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
//import com.gs.fw.common.mithra.test.domain.CalcDate;
//import com.gs.fw.common.mithra.test.domain.CalcDateFinder;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistory;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryFinder;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryList;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantity;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.test.mag.*;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.sql.Timestamp;
import java.util.Set;



public class TestToDatedRelationshipViaColumn extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            PositionAdjustmentHistory.class,
            MagEvent.class, MagTrade.class, MagTransaction.class, MagTransactionPosting.class, MagReportingAttributes.class, MagProductId.class,
//            PositionQuantity.class, CalcDate.class, BitemporalOrder.class
            PositionQuantity.class
        };
    }

    public void testRelationship()
    {
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.referenceId().eq(8004).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)));
        list.forceResolve();
        assertEquals(1, list.size());
        PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(0);
        assertNotNull(hist);
        PositionQuantity pq = hist.getPositionQuantity();
        assertNotNull(pq);
        assertTrue(pq.getBusinessDate().getTime() == hist.getBusinessDate().getTime());
        assertTrue(pq.getBusinessDateFrom().getTime() < hist.getBusinessDate().getTime());
        assertTrue(pq.getBusinessDateTo().getTime() >= hist.getBusinessDate().getTime());
    }

    public void testRelationshipSettingToNull()
    {
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.referenceId().eq(8004).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)));
        list.forceResolve();
        assertEquals(1, list.size());
        PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(0).getNonPersistentCopy();
        hist.setPositionQuantity(null);
        assertNull(hist.getAccountId());
     	assertTrue(hist.isProductIdNull());
        assertNull(hist.getBusinessDate());
    }

    public void testDeepFetch()
    {
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.referenceId().eq(8004).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertEquals(1, list.size());
        PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(0);
        assertNotNull(hist);
        PositionQuantity pq = hist.getPositionQuantity();
        assertNotNull(pq);
        assertTrue(pq.getBusinessDate().getTime() == hist.getBusinessDate().getTime());
        assertTrue(pq.getBusinessDateFrom().getTime() < hist.getBusinessDate().getTime());
        assertTrue(pq.getBusinessDateTo().getTime() >= hist.getBusinessDate().getTime());
    }

    public void testFindByRelated()
    {
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.positionQuantity().quantity().greaterThan(0).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertTrue(list.size() > 0);
        for(int i=0;i<list.size();i++)
        {
            PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(i);
            assertNotNull(hist);
            PositionQuantity pq = hist.getPositionQuantity();
            assertNotNull(pq);
            assertTrue(pq.getBusinessDate().getTime() == hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateFrom().getTime() < hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateTo().getTime() >= hist.getBusinessDate().getTime());
        }

    }

    public void testFindByRelatedWithoutDeepFetch()
    {
        PositionQuantityList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.positionQuantity().quantity().greaterThan(0).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A))).getPositionQuantities();
        assertTrue(list.size() > 0);
    }

    public void testFindByRelatedWithDatedEqualitySubstitute() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-02-01 18:30:00.0").getTime());
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.positionQuantity().quantity().greaterThan(0).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)).and(PositionAdjustmentHistoryFinder.businessDate().eq(businessDate)));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertTrue(list.size() > 0);
        for(int i=0;i<list.size();i++)
        {
            PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(i);
            assertNotNull(hist);
            PositionQuantity pq = hist.getPositionQuantity();
            assertNotNull(pq);
            assertTrue(pq.getBusinessDate().getTime() == hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateFrom().getTime() < hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateTo().getTime() >= hist.getBusinessDate().getTime());
        }

    }

    public void testFindByRelatedWithDatedEqualitySubstitute2() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-02-01 18:30:00.0").getTime());
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.positionQuantitiesWithoutType().quantity().greaterThan(0).
            and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)).and(PositionAdjustmentHistoryFinder.businessDate().eq(businessDate)));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertTrue(list.size() > 0);
        for(int i=0;i<list.size();i++)
        {
            PositionAdjustmentHistory hist = list.getPositionAdjustmentHistoryAt(i);
            assertNotNull(hist);
            PositionQuantity pq = hist.getPositionQuantity();
            assertNotNull(pq);
            assertTrue(pq.getBusinessDate().getTime() == hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateFrom().getTime() < hist.getBusinessDate().getTime());
            assertTrue(pq.getBusinessDateTo().getTime() >= hist.getBusinessDate().getTime());
        }

    }

    public void testSpecifyingAsOfAttributeOnTheRight() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-27 18:30:00.0").getTime());
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)
                .and(PositionAdjustmentHistoryFinder.positionQuantity().businessDate().eq(businessDate)));
        list.forceResolve();
        assertEquals(2, list.size());
        assertEquals(businessDate, list.get(0).getBusinessDate());

        list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.positionQuantitiesWithoutType().quantity().greaterThan(0).
                and(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)).and(PositionAdjustmentHistoryFinder.positionQuantity().businessDate().eq(businessDate)));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertEquals(2, list.size());
        int count = this.getRetrievalCount();
        assertNotNull(list.get(0).getPositionQuantity());
        assertEquals(count, this.getRetrievalCount());
    }

    public void testTwoLevelDeepStartingFromNonDated() throws Exception
    {
        Timestamp tradeDate = new Timestamp(timestampFormat.parse("2013-09-04 00:00:00").getTime());
        Operation op = MagEventFinder.magellanSourceInstanceId().eq(0);
        op = op.and(MagEventFinder.transaction().trade().tradeDate().eq(tradeDate));
        op = op.and(MagEventFinder.eventBusinessDate().lessThanEquals(tradeDate));
        MagEventList magEvents = MagEventFinder.findMany(op);
        magEvents.deepFetch(MagEventFinder.transaction().trade());
        assertEquals(11, magEvents.size());
        int retrievalCount = this.getRetrievalCount();
        for(int i=0;i<11;i++)
        {
            assertNotNull(magEvents.get(i).getTransaction().getTrade());
        }
        assertEquals(retrievalCount, this.getRetrievalCount());
    }

    public void testThreeLevelDeepStartingFromNonDated() throws Exception
    {
        Timestamp tradeDate = new Timestamp(timestampFormat.parse("2013-11-26 00:00:00").getTime());
        IntHashSet accountIdSet = IntHashSet.newSetWith(39955000);
        final Set<String> exchangeCodeSet = UnifiedSet.newSetWith("AMEX", "BOSE");

        Operation op = MagEventFinder.magellanSourceInstanceId().eq(0);
        op = op.and(MagEventFinder.eventBusinessDate().lessThanEquals(tradeDate));
        op = op.and(MagEventFinder.eventBusinessDate().greaterThanEquals(tradeDate));
        op = op.and(MagEventFinder.transaction().accountId().in(accountIdSet));
        op = op.and(MagEventFinder.transaction().reportingAttributes().exchangeCode().in(exchangeCodeSet));

        MagEventList magEvents = MagEventFinder.findMany(op);
        magEvents.deepFetch(MagEventFinder.transaction().trade().productId());
        assertEquals(2, magEvents.size());
        int retrievalCount = this.getRetrievalCount();
        for(int i=0;i<2;i++)
        {
            assertNotNull(magEvents.get(i).getTransaction().getTrade());
            assertNotNull(magEvents.get(i).getTransaction().getTrade().getProductId());
        }
        assertEquals(retrievalCount, this.getRetrievalCount());
    }

    public void testThreeLevelDeepStartingFromNonDatedTripleInClause() throws Exception
    {
        Timestamp tradeDate = new Timestamp(timestampFormat.parse("2013-11-26 00:00:00").getTime());
        IntHashSet accountIdSet = IntHashSet.newSetWith(39955000);
        final Set<String> exchangeCodeSet = UnifiedSet.newSetWith("AMEX", "BOSE");
        IntHashSet primeIdSet = IntHashSet.newSetWith(0, 1, 2, 370227706, 1003032317);

        Operation op = MagEventFinder.magellanSourceInstanceId().eq(0);
        op = op.and(MagEventFinder.eventBusinessDate().lessThanEquals(tradeDate));
        op = op.and(MagEventFinder.eventBusinessDate().greaterThanEquals(tradeDate));
        op = op.and(MagEventFinder.transaction().accountId().in(accountIdSet));
        op = op.and(MagEventFinder.transaction().reportingAttributes().exchangeCode().in(exchangeCodeSet));
        op = op.and(MagEventFinder.transaction().trade().productId().primeId().in(primeIdSet));

        MagEventList magEvents = MagEventFinder.findMany(op);
        magEvents.deepFetch(MagEventFinder.transaction().trade().productId());
        assertEquals(2, magEvents.size());
        int retrievalCount = this.getRetrievalCount();
        for(int i=0;i<2;i++)
        {
            assertNotNull(magEvents.get(i).getTransaction().getTrade());
            assertNotNull(magEvents.get(i).getTransaction().getTrade().getProductId());
        }
        assertEquals(retrievalCount, this.getRetrievalCount());
    }

    public void testSpecifyingAsOfAttributeOnTheLeft() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-27 18:30:00.0").getTime());
        PositionQuantityList list = PositionQuantityFinder.findMany(PositionQuantityFinder.acmapCode().eq(SOURCE_A).and(
                PositionQuantityFinder.businessDate().eq(businessDate).and(PositionQuantityFinder.adjustmentHistory1800().exists())));
        list.deepFetch(PositionQuantityFinder.adjustmentHistory1800());
        list.forceResolve();
        assertEquals(2, list.size());
    }

    //todo: this test is currently broken. It has to generate a strange SQL: t0.buzDate = t1.to, because that's t1.to is the edge point.
    public void xtestSpecifyingAsOfEdgePointOnTheRight() throws Exception
    {
        PositionAdjustmentHistoryList list = new PositionAdjustmentHistoryList(PositionAdjustmentHistoryFinder.acmapCode().eq(SOURCE_A)
                .and(PositionAdjustmentHistoryFinder.positionQuantity().businessDate().equalsEdgePoint()));
        list.deepFetch(PositionAdjustmentHistoryFinder.positionQuantity());
        list.forceResolve();
        assertEquals(0, list.size());

    }

//    public void testAggregationViaCalcDate() throws Exception
//    {
//        Timestamp startDate = new Timestamp(timestampFormat.parse("2002-01-27 18:30:00.0").getTime());
//        Operation op = CalcDateFinder.calcDate().greaterThan(startDate);
//        AggregateList list = new AggregateList(op);
//        list.addAggregateAttribute("sum", CalcDateFinder.justByDate().orderId().sum());
//        list.addGroupBy("userId", CalcDateFinder.justByDate().userId());
//        list.forceResolve();
//    }

}

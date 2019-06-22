package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderStatusFinder;
import org.junit.Assert;

public class TestCachedQueryClass extends MithraTestAbstract
{
    public void testUpdateCountersCopyFromFirstReflectsFirstBecomingStale()
    {
        // These two ops are unrelated so have different update count holders
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderStatusFinder.status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery secondCopiedFromFirst = new CachedQuery(secondOp, secondOrderBy, first); // note: copy from first, not merge

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(secondCopiedFromFirst.isExpired());

        // Invalidate the first cached query
        OrderFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertTrue(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertTrue(secondCopiedFromFirst.isExpired());

        final CachedQuery newSecondCopiedFromFirst = new CachedQuery(secondOp, secondOrderBy, first);
        Assert.assertTrue(newSecondCopiedFromFirst.isExpired());// should be expired even though newly created, as first is still stale
    }

    public void testUpdateCountersCopyFromFirstDoesNotReflectUpdateCountsOfSecond()
    {
        // These two ops are unrelated so have different update count holders
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderStatusFinder.status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery secondCopiedFromFirst = new CachedQuery(secondOp, secondOrderBy, first); // note: copy from first, not merge

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(secondCopiedFromFirst.isExpired());

        // Invalidate the second cached query
        OrderStatusFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertFalse(first.isExpired());
        Assert.assertTrue(second.isExpired());
        Assert.assertFalse(secondCopiedFromFirst.isExpired());
    }

    public void testUpdateCountersMergeOfUnrelatedOpsIncludesFirst()
    {
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderStatusFinder.status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // Invalidate the first cached query
        OrderFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertTrue(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertTrue(combined.isExpired());

        final CachedQuery newCombined = new CachedQuery(secondOp, secondOrderBy, first, true);
        Assert.assertTrue(newCombined.isExpired());// should be expired even though newly created, as first is still stale
    }

    public void testUpdateCountersMergeOfUnrelatedOpsIncludesSecond()
    {
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderStatusFinder.status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // Invalidate the second cached query
        OrderStatusFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertFalse(first.isExpired());
        Assert.assertTrue(second.isExpired());
        Assert.assertTrue(combined.isExpired());

        final CachedQuery newCombined = new CachedQuery(secondOp, secondOrderBy, first, true);
        Assert.assertFalse(newCombined.isExpired()); // should not be expired as newCombined contains the latest update counters for the second operation
    }

    public void testUpdateCountersMergeOfRelatedOpsRetainsTheOlderUpdateCountOfFirst()
    {
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderFinder.orderStatus().status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        Assert.assertFalse(first.isExpired());

        // Invalidate the parent table which both queries should depend on, before the second cached query has snapshotted it
        OrderFinder.getMithraObjectPortal().incrementClassUpdateCount();

        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertTrue(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertTrue(combined.isExpired());
    }

    public void testUpdateCountersMergeOfRelatedOpsMergesEquivalentCounts()
    {
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderFinder.orderStatus().status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // Invalidate the parent table which both queries should depend on
        OrderFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertTrue(first.isExpired());
        Assert.assertTrue(second.isExpired());
        Assert.assertTrue(combined.isExpired());
    }

    public void testUpdateCountersMergeOfRelatedOpsIncludesAdditionalDependenciesOfSecond()
    {
        final Operation firstOp = OrderFinder.state().eq("In-Progress");
        final Operation secondOp = OrderFinder.orderStatus().status().eq(10);

        final OrderBy firstOrderBy = OrderFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // Invalidate the child table which only the second op should depend on
        OrderStatusFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertFalse(first.isExpired());
        Assert.assertTrue(second.isExpired());
        Assert.assertTrue(combined.isExpired());

        final CachedQuery newCombined = new CachedQuery(secondOp, secondOrderBy, first, true);
        Assert.assertFalse(newCombined.isExpired()); // should not be expired as newCombined contains the latest update counters for the second operation
    }

    public void testUpdateCountersMergeOnTempObjectAsFirst()
    {
        final TupleTempContext tupleTempContext = new TupleTempContext(OrderFinder.allPersistentAttributes(), true);
        final Operation firstOp = tupleTempContext.all();
        final Operation secondOp = OrderStatusFinder.status().eq(10);

        final OrderBy firstOrderBy = tupleTempContext.getTupleAttributesAsAttributeArray()[0].ascendingOrderBy();
        final OrderBy secondOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // There is no way to invalidate a temp context cached query - it has no update counters.
        // Invalidate the second cached query
        OrderStatusFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertFalse(first.isExpired());
        Assert.assertTrue(second.isExpired());
        Assert.assertTrue(combined.isExpired());

        final CachedQuery newCombined = new CachedQuery(secondOp, secondOrderBy, first, true);
        Assert.assertFalse(newCombined.isExpired()); // should not be expired as newCombined contains the latest update counters for the second operation
    }

    public void testUpdateCountersMergeOnTempObjectAsSecond()
    {
        final TupleTempContext tupleTempContext = new TupleTempContext(OrderFinder.allPersistentAttributes(), true);
        final Operation firstOp = OrderStatusFinder.status().eq(10);
        final Operation secondOp = tupleTempContext.all();

        final OrderBy firstOrderBy = OrderStatusFinder.orderId().ascendingOrderBy();
        final OrderBy secondOrderBy = tupleTempContext.getTupleAttributesAsAttributeArray()[0].ascendingOrderBy();

        final CachedQuery first = new CachedQuery(firstOp, firstOrderBy);
        final CachedQuery second = new CachedQuery(secondOp, secondOrderBy);
        final CachedQuery combined = new CachedQuery(secondOp, secondOrderBy, first, true);

        Assert.assertFalse(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertFalse(combined.isExpired());

        // There is no way to invalidate a temp context cached query - it has no update counters.
        // Invalidate the first cached query
        OrderStatusFinder.getMithraObjectPortal().incrementClassUpdateCount();

        Assert.assertTrue(first.isExpired());
        Assert.assertFalse(second.isExpired());
        Assert.assertTrue(combined.isExpired());

        final CachedQuery newCombined = new CachedQuery(secondOp, secondOrderBy, first, true);
        Assert.assertTrue(newCombined.isExpired()); // should be expired even though newly created, as first is still stale
    }
}

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

import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.ParaBalance;
import com.gs.fw.common.mithra.test.domain.ParaBalanceFinder;
import com.gs.fw.common.mithra.test.domain.ParaBalanceList;
import junit.framework.Assert;


public class TestTransactionalAdhocFastList extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        ParaBalance.class
                };
    }


    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public void testPurgeAllEmptyList()
    {
        ParaBalanceList balances = new ParaBalanceList();
        balances.purgeAll();
        // should not fail, no specific asserts required
    }

    public void testPurgeAllListWithOneItem()
    {
        Operation allBalances = ParaBalanceFinder.acmapCode().eq("A").
                and(ParaBalanceFinder.processingDate().equalsEdgePoint()).
                and(ParaBalanceFinder.businessDate().equalsEdgePoint());

        ParaBalanceList balances = ParaBalanceFinder.findMany(allBalances);
        ParaBalanceList itemsToPurge = new ParaBalanceList();
        int sizeBefore = balances.size();
        for (ParaBalance paraBalance : balances)
        {
            if (paraBalance.getProcessingDate().before(InfinityTimestamp.getParaInfinity()))
            {
                itemsToPurge.add(paraBalance);

            }
        }
        int purgeSize = itemsToPurge.size();
        Assert.assertEquals("Incorrect number of Items to Purge", 1, purgeSize);
        itemsToPurge.purgeAll();

        ParaBalanceList afterPurgeList = ParaBalanceFinder.findMany(allBalances);
        int sizeAfter = afterPurgeList.count();
        Assert.assertTrue((sizeBefore) == (sizeAfter + purgeSize));


    }


    public void testPurgeAllListWithMultipleItems()
    {
        Operation allBalances = ParaBalanceFinder.acmapCode().eq("A").
                and(ParaBalanceFinder.processingDate().equalsEdgePoint()).
                and(ParaBalanceFinder.businessDate().equalsEdgePoint());

        ParaBalanceList balances = ParaBalanceFinder.findMany(allBalances);
        ParaBalanceList itemsToPurge = new ParaBalanceList();
        ParaBalance toBePurged = null;
        ParaBalance copy = null;
        int sizeBefore = balances.size();
        for (ParaBalance paraBalance : balances)
        {
            if (!paraBalance.getProcessingDate().before(InfinityTimestamp.getParaInfinity()))
            {
                itemsToPurge.add(paraBalance);
                toBePurged = paraBalance;
                copy = paraBalance.getNonPersistentCopy();
            }
        }
        int purgeSize = itemsToPurge.size();
        Assert.assertTrue("Incorrect number of Items to Purge", purgeSize > 1);

        itemsToPurge.purgeAll();

        ParaBalanceList afterPurgeList = ParaBalanceFinder.findMany(allBalances);
        int sizeAfter = afterPurgeList.count();
        Assert.assertTrue((sizeBefore) == (sizeAfter + purgeSize));
        try
        {
            toBePurged.getBalanceId();
            fail("didn't throw exception");
        }
        catch(MithraDeletedException e)
        {

            //ignore
        }
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().eq(copy.getBalanceId()));
        op = op.and(ParaBalanceFinder.processingDate().eq(copy.getProcessingDate()));
        op = op.and(ParaBalanceFinder.businessDate().eq(copy.getBusinessDate()));
        assertNull(ParaBalanceFinder.findOne(op));


    }

}

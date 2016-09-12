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

import java.sql.Timestamp;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.block.procedure.primitive.ObjectIntProcedure;
import com.gs.fw.common.mithra.test.domain.*;
import junit.framework.TestCase;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.Operation;



public class TestCrossDatabaseAdhocDeepFetch extends MithraTestAbstract
{    
    public void testOneToManyOneToOneSameAttributeAdhoc()
    {
        ParaPositionList nonOpList = getDeepFetchedNonOpList();
        assertEquals(9, nonOpList.size());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(ParaPosition p: nonOpList)
        {
            p.getAccount();
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testForEachForcesResolve()
    {
        ParaPositionList nonOpList = getDeepFetchedNonOpList();
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        nonOpList.asGscList().forEach(new Procedure<ParaPosition>()
        {
            @Override
            public void value(ParaPosition paraPosition)
            {
                paraPosition.getAccount();
            }
        });
        // expect 1 extra retrieve for partial cache and 0 for full cache
        assertTrue(MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() <= count + 1);
    }    

    public void testForEachWithIndexForcesResolve()
    {
        ParaPositionList nonOpList = getDeepFetchedNonOpList();
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        nonOpList.asGscList().forEachWithIndex(new ObjectIntProcedure<ParaPosition>()
        {
            @Override
            public void value(ParaPosition paraPosition, int index)
            {
                paraPosition.getAccount();
            }
        });
        // expect 1 extra retrieve for partial cache and 0 for full cache
        assertTrue(MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() <= count + 1);
    }    

    public void testForEachWithForcesResolve()
    {
        ParaPositionList nonOpList = getDeepFetchedNonOpList();
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        nonOpList.asGscList().forEachWith(new Procedure2<ParaPosition, Object>()
        {
            @Override
            public void value(ParaPosition paraPosition, Object o)
            {
                paraPosition.getAccount();
            }
        }, new Object());
        // expect 1 extra retrieve for partial cache and 0 for full cache
        assertTrue(MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() <= count + 1);
    }    

    private ParaPositionList getDeepFetchedNonOpList()
    {
        Operation operation = ParaPositionFinder.businessDate().eq(Timestamp.valueOf("2011-09-30 23:59:00.0"));
        ParaPositionList nonOpList = new ParaPositionList(ParaPositionFinder.findMany(operation));
        nonOpList.deepFetch(ParaPositionFinder.account());
        return nonOpList;
    }

    public void testOneToManyOneToOneSameAttribute()
    {
        Timestamp businessDate = Timestamp.valueOf("2011-09-30 23:59:00.0");
        ParaPositionList list = new ParaPositionList();
        for(int i=0;i<2000;i++)
        {
            ParaPosition pos = new ParaPosition(InfinityTimestamp.getParaInfinity());
            pos.setAccountNumber("777"+i);
            pos.setAccountSubtype(""+(i % 10));
            pos.setBusinessDate(businessDate);
            pos.setProductIdentifier("P"+i);
            list.add(pos);
        }
        list.insertAll();
        Operation operation = ParaPositionFinder.businessDate().eq(businessDate);
        ParaPositionList nonOpList = ParaPositionFinder.findMany(operation);
        nonOpList.deepFetch(ParaPositionFinder.account());
        assertEquals(2009, nonOpList.size());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(ParaPosition p: nonOpList)
        {
            p.getAccount();
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }    

    public void testHugeInClauseWithSourelessToSourceDeepFetch() throws Exception
    {
        ProductList list = new ProductList();
        for (int i=0;i<1007;i++)
        {
            Product product = new Product();
            product.setProductId(i);
            list.add(product);
        }

        Timestamp buzDate = new Timestamp(timestampFormat.parse("2010-10-11 00:00:00").getTime());
        list.deepFetch(ProductFinder.positions("A", buzDate));
        list.forceResolve();
        int retrievalCount = getRetrievalCount();
        for(int i=0;i<list.size();i++)
        {
            list.get(i).getPositions("A", buzDate);
        }
        assertEquals(retrievalCount, getRetrievalCount());
    }
}
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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraUpdateListenerAbstract;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Division;
import com.gs.fw.common.mithra.test.domain.DivisionFinder;
import com.gs.fw.common.mithra.test.domain.Trade;
import com.gs.fw.common.mithra.test.domain.TradeFinder;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;



public class TestUpdateListener extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Division.class,
            Trade.class,
        };
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        MithraUpdateListenerAbstract.setContextData(null);
    }

    public void testUpdateListener()
    {
        final Operation op = DivisionFinder.sourceId().eq("A").and(DivisionFinder.divisionId().eq(100));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                MithraUpdateListenerAbstract.setContextData("California");
                Division div = DivisionFinder.findOne(op);
                div.setCity("San Diego");
                return null;
            }
        });
        Division div = DivisionFinder.findOne(op);
        assertEquals("California", div.getState());
        div = DivisionFinder.findOneBypassCache(op);
        assertEquals("California", div.getState());
    }

    public void testUpdateListenerViaDetached()
    {
        final Operation op = DivisionFinder.sourceId().eq("A").and(DivisionFinder.divisionId().eq(100));
        final Division detachedDiv = DivisionFinder.findOne(op).getDetachedCopy();
        detachedDiv.setCity("San Diego");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                MithraUpdateListenerAbstract.setContextData("California");
                detachedDiv.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        Division div = DivisionFinder.findOne(op);
        assertEquals("California", div.getState());
        div = DivisionFinder.findOneBypassCache(op);
        assertEquals("California", div.getState());
    }

    public void testUpdateListenerViaCopy()
    {
        final Operation op = DivisionFinder.sourceId().eq("A").and(DivisionFinder.divisionId().eq(100));
        final Division div = DivisionFinder.findOne(op);
        final Division copy = DivisionFinder.findOne(op).getNonPersistentCopy();
        copy.setCity("San Diego");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                MithraUpdateListenerAbstract.setContextData("California");
                div.copyNonPrimaryKeyAttributesFrom(copy);
                return null;
            }
        });
        Division testDiv = DivisionFinder.findOne(op);
        assertEquals("California", testDiv.getState());
        testDiv = DivisionFinder.findOneBypassCache(op);
        assertEquals("California", testDiv.getState());
    }

    public void testDatedUpdateListener()
    {
        Timestamp lewBusinessDate = Timestamp.valueOf("2007-05-15 23:59:00.000");

        final Operation op = TradeFinder.businessDate().eq(lewBusinessDate).and(TradeFinder.tradeId().eq(0));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Trade trade = TradeFinder.findOne(op);
                trade.setTradeRef("San Diego");
                return null;
            }
        });
        Trade testTrade = TradeFinder.findOne(op);
        assertEquals(1234, testTrade.getCreateCode());
        testTrade = TradeFinder.findOneBypassCache(op);
        assertEquals(1234, testTrade.getCreateCode());
    }

    public void testDatedUpdateListenerViaDetached()
    {
        Timestamp lewBusinessDate = Timestamp.valueOf("2007-05-15 23:59:00.000");

        final Operation op = TradeFinder.businessDate().eq(lewBusinessDate).and(TradeFinder.tradeId().eq(0));
        final Trade detachedTrade = TradeFinder.findOne(op).getDetachedCopy();
        detachedTrade.setTradeRef("San Diego");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                detachedTrade.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        Trade testTrade = TradeFinder.findOne(op);
        assertEquals(1234, testTrade.getCreateCode());
        testTrade = TradeFinder.findOneBypassCache(op);
        assertEquals(1234, testTrade.getCreateCode());
    }

    public void testDatedUpdateListenerViaCopy()
    {
        Timestamp lewBusinessDate = Timestamp.valueOf("2007-05-15 23:59:00.000");

        final Operation op = TradeFinder.businessDate().eq(lewBusinessDate).and(TradeFinder.tradeId().eq(0));
        final Trade trade = TradeFinder.findOne(op);
        final Trade copy = TradeFinder.findOne(op).getNonPersistentCopy();
        copy.setTradeRef("San Diego");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                trade.copyNonPrimaryKeyAttributesFrom(copy);
                return null;
            }
        });
        Trade testTrade = TradeFinder.findOne(op);
        assertEquals(1234, testTrade.getCreateCode());
        testTrade = TradeFinder.findOneBypassCache(op);
        assertEquals(1234, testTrade.getCreateCode());
    }


}
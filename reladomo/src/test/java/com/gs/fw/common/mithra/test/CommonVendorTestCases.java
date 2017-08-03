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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class CommonVendorTestCases extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void testRollback()
    {
        final List<Exception> exceptionsList = FastList.newList();
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();

                try
                {
                    Order order = new Order();
                    order.setOrderId(1000);
                    order.insert();
                    if (exceptionsList.isEmpty())
                    {
                        MithraBusinessException exception = new MithraBusinessException("Exception");
                        exception.setRetriable(true);
                        exceptionsList.add(exception);
                        throw exception;
                    }
                    return null;
                }
                finally
                {
                    temporaryContext.destroy();
                }
            }
        }, 5);
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(1000)));
    }

    public void testTimestampGranularity() throws Exception
    {
        TimestampConversionList list = new TimestampConversionList();
        long start = timestampFormat.parse("2017-08-03 11:51:24").getTime();
        for(int i=0;i<1000;i++)
        {
            TimestampConversion conversion = new TimestampConversion();
            conversion.setId(1000+i);
            conversion.setTimestampValueDB(new Timestamp(start + i*10));
            conversion.setTimestampValueNone(new Timestamp(start + i*10));
            conversion.setTimestampValueUTC(new Timestamp(start + i*10));
            list.add(conversion);
        }
        list.insertAll();

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        TimestampConversionList many = TimestampConversionFinder.findMany(TimestampConversionFinder.id().greaterThanEquals(1000));
        many.setOrderBy(TimestampConversionFinder.id().ascendingOrderBy());

        for(int i=0;i<1000;i++)
        {
            TimestampConversion timestampConversion = list.get(i);
            assertEquals(start + i*10, timestampConversion.getTimestampValueDB().getTime());
            assertEquals(start + i*10, timestampConversion.getTimestampValueNone().getTime());
            assertEquals(start + i*10, timestampConversion.getTimestampValueUTC().getTime());
        }

        for(int i=0;i<1000;i++)
        {
            assertEquals(1000 + i, TimestampConversionFinder.findOneBypassCache(TimestampConversionFinder.timestampValueDB().eq(new Timestamp(start + i * 10))).getId());
            assertEquals(1000 + i, TimestampConversionFinder.findOneBypassCache(TimestampConversionFinder.timestampValueNone().eq(new Timestamp(start + i * 10))).getId());
            assertEquals(1000 + i, TimestampConversionFinder.findOneBypassCache(TimestampConversionFinder.timestampValueUTC().eq(new Timestamp(start + i * 10))).getId());
        }
    }

    public void testOptimisticLocking()
    {
        final TestEodAcctIfPnlList list = TestEodAcctIfPnlFinder.findMany(TestEodAcctIfPnlFinder.all());
        int size = list.size();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TestEodAcctIfPnlFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                for(TestEodAcctIfPnl pnl: list)
                {
                    pnl.setUserId("fred");
                }
                return null;
            }
        });

        assertEquals(size, TestEodAcctIfPnlFinder.findMany(TestEodAcctIfPnlFinder.userId().eq("fred")).size());
    }

}

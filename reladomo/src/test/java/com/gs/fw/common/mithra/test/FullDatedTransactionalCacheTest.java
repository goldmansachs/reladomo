

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
import java.text.SimpleDateFormat;
import java.util.List;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.cache.FullDatedTransactionalCache;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceData;
import com.gs.fw.common.mithra.test.domain.TinyBalanceDatabaseObject;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import junit.framework.Assert;
import junit.framework.TestCase;



public class FullDatedTransactionalCacheTest extends TestCase
{

    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void testDuplicate() throws Exception
    {
        FullDatedTransactionalCache cache = new FullDatedTransactionalCache(TinyBalanceFinder.getPrimaryKeyAttributes(), TinyBalanceFinder.getAsOfAttributes(), new TinyBalanceDatabaseObject(), TinyBalanceFinder.getImmutableAttributes());
        Timestamp businessDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp processingDateFromToCreateDuplicate = new Timestamp(timestampFormat.parse("2002-01-03 10:00:00").getTime());
        for (int i = 0; i < 5; i++)
        {
            TinyBalance data = new TinyBalance(businessDateFrom, businessDateFrom);
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            cache.put(data);
        }
        for (int i = 0; i < 5; i++)
        {
            TinyBalance data = new TinyBalance(businessDateFrom, businessDateFrom);
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());

            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFromToCreateDuplicate);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());
            cache.put(data);
        }
        for (int i = 50; i < 55; i++)
        {
            TinyBalance data = new TinyBalance(businessDateFrom, businessDateFrom);
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            cache.put(data);
        }
        Assert.assertEquals(15, cache.size());
        List<Object> duplicates = cache.collectMilestoningOverlaps();
        Assert.assertEquals(10, duplicates.size());
        for (int i = 0; i < 10; i = i + 2)
        {
            TinyBalanceData originalData = (TinyBalanceData) duplicates.get(i);
            TinyBalanceData duplicateData = (TinyBalanceData) duplicates.get(i + 1);
            Assert.assertEquals(originalData.getBalanceId(), duplicateData.getBalanceId());
        }
    }
}
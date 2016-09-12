

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

import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import com.gs.fw.common.mithra.cache.PartialDatedCache;
import com.gs.fw.common.mithra.test.domain.*;
import junit.framework.Assert;
import junit.framework.TestCase;

public class PartialDatedCacheTest extends TestCase
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void testDuplicateForPartialCacheAndFullyMilestonedData() throws Exception
    {
        PartialDatedCache cache = new PartialDatedCache(TinyBalanceFinder.getPrimaryKeyAttributes(), TinyBalanceFinder.getAsOfAttributes(), new TinyBalanceDatabaseObject(), TinyBalanceFinder.getImmutableAttributes(), 100000, 1000000);
        Timestamp businessDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 00:00:00").getTime());
        Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp processingDateFromToCreateDuplicate = new Timestamp(timestampFormat.parse("2002-01-03 10:00:00").getTime());
        for (int i = 0; i < 5; i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        for (int i = 0; i < 5; i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFromToCreateDuplicate);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        for (int i = 50; i < 55; i++)
        {
            TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
            data.setBusinessDateFrom(businessDateFrom);
            data.setBusinessDateTo(TinyBalanceFinder.businessDate().getInfinityDate());
            data.setBalanceId(i);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(TinyBalanceFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        Assert.assertEquals(15, cache.size());
        List<Object> duplicates = cache.collectMilestoningOverlaps();
        Assert.assertEquals(10, duplicates.size());
        for (int i = 0; i < 10; i = i + 2)
        {
            TinyBalanceData originalData = (TinyBalanceData) ((WeakReference) duplicates.get(i)).get();
            TinyBalanceData duplicateData = (TinyBalanceData) ((WeakReference) duplicates.get(i + 1)).get();
            Assert.assertEquals(originalData.getBalanceId(), duplicateData.getBalanceId());
        }
    }

    public void testDuplicateForPartialCacheAndPartialMilestonedData() throws Exception
    {

        PartialDatedCache cache = new PartialDatedCache(AuditedGroupFinder.getPrimaryKeyAttributes(), AuditedGroupFinder.getAsOfAttributes(), new AuditedGroupDatabaseObject(), AuditedGroupFinder.getImmutableAttributes(), 10000, 100000);
        Timestamp processingDateFrom = new Timestamp(timestampFormat.parse("2002-01-02 10:00:00").getTime());
        Timestamp processingDateFromToCreateDuplicate = new Timestamp(timestampFormat.parse("2002-01-03 10:00:00").getTime());
        for (int i = 0; i < 5; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setSourceId(1);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        for (int i = 0; i < 5; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setSourceId(1);
            data.setProcessingDateFrom(processingDateFromToCreateDuplicate);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        for (int i = 50; i < 55; i++)
        {
            AuditedGroupData data = AuditedGroupDatabaseObject.allocateOnHeapData();
            data.setId(i);
            data.setSourceId(1);
            data.setProcessingDateFrom(processingDateFrom);
            data.setProcessingDateTo(AuditedGroupFinder.processingDate().getInfinityDate());

            cache.putDatedData(data);
        }
        Assert.assertEquals(15, cache.size());
        List<Object> duplicates = cache.collectMilestoningOverlaps();
        Assert.assertEquals(10, duplicates.size());
    }
}

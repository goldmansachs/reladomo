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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.cache.AbstractDatedCache;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Timestamp;
import java.util.*;

public class TestRuntimeCacheController
extends MithraTestAbstract
{

    public void testCacheArchiveTransactional() throws Exception
    {
        Map<String, MithraRuntimeCacheController> classToControllerMap = new HashMap<String, MithraRuntimeCacheController>();
        Set<MithraRuntimeCacheController> controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        for(MithraRuntimeCacheController cont: controllerSet)
        {
            classToControllerMap.put(cont.getClassName(), cont);
        }
        MithraRuntimeCacheController cacheController = classToControllerMap.get(Order.class.getName());
        OrderList orders = OrderFinder.findMany(OrderFinder.all());
        orders.forceResolve();
        int sizeBefore = orders.size();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
        if (cacheController.isPartialCache())
        {
            cacheController.archiveObjects(baos, orders);
        }
        else
        {
            cacheController.archiveCache(baos, null);
        }
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        cacheController.readCacheFromArchive(bais);
        OrderList orders2 = OrderFinder.findMany(OrderFinder.all());
        assertEquals(sizeBefore, orders2.size());
    }

    public void testCacheArchiveDated() throws Exception
    {
        Map<String, MithraRuntimeCacheController> classToControllerMap = new HashMap<String, MithraRuntimeCacheController>();
        Set<MithraRuntimeCacheController> controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        for(MithraRuntimeCacheController cont: controllerSet)
        {
            classToControllerMap.put(cont.getClassName(), cont);
        }
        MithraRuntimeCacheController cacheController = classToControllerMap.get(TinyBalance.class.getName());
        TinyBalanceList balances = this.getCachedTinyBalances();
        int sizeBefore = balances.size();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
        if (cacheController.isPartialCache())
        {
            cacheController.archiveObjects(baos, balances);
        }
        else
        {
            cacheController.archiveCache(baos, null);
        }

        assertFalse(cacheController.getMithraObjectPortal().isReplicated());
        cacheController.getMithraObjectPortal().getCache().clear();
        cacheController.clearQueryCache();

        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        cacheController.readCacheFromArchive(bais);
        TinyBalanceList balances2 = this.getCachedTinyBalances();
        assertEquals(sizeBefore, balances2.size());
    }

    public void testCacheArchiveDatedWithFilter() throws Exception
    {
        AbstractDatedCache.zLIST_CHUNK_SIZE = 7;

        Map<String, MithraRuntimeCacheController> classToControllerMap = new HashMap<String, MithraRuntimeCacheController>();
        Set<MithraRuntimeCacheController> controllerSet = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        for(MithraRuntimeCacheController cont: controllerSet)
        {
            classToControllerMap.put(cont.getClassName(), cont);
        }
        MithraRuntimeCacheController cacheController = classToControllerMap.get(TinyBalance.class.getName());
        if (cacheController.isPartialCache())
        {    // because partial cache will reread objects from H2 and return 85 balances every time.
            return;
        }

        Timestamp dateToKeep = Timestamp.valueOf("2006-02-01 00:00:00");

        int filteredCount = 0;
        for (TinyBalance each : this.getCachedTinyBalances())
        {
            if (each.getBusinessDateFrom().getTime() <= dateToKeep.getTime()
                    && dateToKeep.getTime() < each.getBusinessDateTo().getTime()) filteredCount++;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);

        Filter filterOut = new KeepOnlySpecifiedDatesFilter(TinyBalanceFinder.businessDate(),
                FastList.newListWith(dateToKeep));
        cacheController.archiveCache(baos, filterOut);

        cacheController.getMithraObjectPortal().getCache().clear();
        cacheController.clearQueryCache();

        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        cacheController.readCacheFromArchive(bais);
        assertEquals(filteredCount, this.getCachedTinyBalances().size());
    }

    public void tearDown ()
    {
        AbstractDatedCache.zLIST_CHUNK_SIZE = 1000;
    }

    private TinyBalanceList getCachedTinyBalances ()
    {
        return TinyBalanceFinder.findMany(TinyBalanceFinder.processingDate().equalsEdgePoint().and(TinyBalanceFinder.businessDate().equalsEdgePoint()).and(
                TinyBalanceFinder.acmapCode().in(new HashSet(Arrays.asList(new String[]{"A", "B"})))));
    }
}

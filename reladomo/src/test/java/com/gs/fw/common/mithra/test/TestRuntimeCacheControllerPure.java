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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.cache.AbstractDatedCache;
import com.gs.fw.common.mithra.test.domain.PureBitemporalOrder;
import com.gs.fw.common.mithra.test.domain.PureBitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.PureBitemporalOrderList;
import com.gs.fw.common.mithra.test.domain.PureOrder;
import com.gs.fw.common.mithra.test.domain.PureOrderFinder;
import com.gs.fw.common.mithra.test.domain.PureOrderList;
import com.gs.fw.common.mithra.test.domain.pure.PureType2DatedReadOnlyTypesA;
import com.gs.fw.common.mithra.test.domain.pure.PureType2DatedReadOnlyTypesAFinder;
import com.gs.fw.common.mithra.test.domain.pure.PureType2DatedReadOnlyTypesAList;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestRuntimeCacheControllerPure
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
        MithraRuntimeCacheController cacheController = classToControllerMap.get(PureOrder.class.getName());
        PureOrderList orders = PureOrderFinder.findMany(PureOrderFinder.all());
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
        PureOrderList orders2 = PureOrderFinder.findMany(PureOrderFinder.all());
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
        MithraRuntimeCacheController cacheController = classToControllerMap.get(PureBitemporalOrder.class.getName());
        PureBitemporalOrderList orders = this.getCachedPureBitemporalOrders();
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

        cacheController.getMithraObjectPortal().getCache().clear();
        cacheController.clearQueryCache();

        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        cacheController.readCacheFromArchive(bais);
        PureBitemporalOrderList orders2 = this.getCachedPureBitemporalOrders();
        assertEquals(sizeBefore, orders2.size());
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
        MithraRuntimeCacheController cacheController = classToControllerMap.get(PureType2DatedReadOnlyTypesA.class.getName());
        if (cacheController.isPartialCache())
        {
            return;
        }

        Timestamp dateToKeep = Timestamp.valueOf("2007-09-15 18:30:00");

        int filteredCount = 0;
        for (PureType2DatedReadOnlyTypesA each : this.getCachedPureType2DatedReadOnlyTypesA())
        {
            if (each.getBusinessDateFrom().getTime() <= dateToKeep.getTime()
                &&
                dateToKeep.getTime() < each.getBusinessDateTo().getTime())
            {
                filteredCount++;
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);

        Filter filterOut = new KeepOnlySpecifiedDatesFilter(PureType2DatedReadOnlyTypesAFinder.businessDate(), FastList.newListWith(dateToKeep));
        cacheController.archiveCache(baos, filterOut);

        cacheController.getMithraObjectPortal().getCache().clear();
        cacheController.clearQueryCache();

        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        cacheController.readCacheFromArchive(bais);
        assertEquals(filteredCount, this.getCachedPureType2DatedReadOnlyTypesA().size());
    }

    public void tearDown ()
    {
        AbstractDatedCache.zLIST_CHUNK_SIZE = 1000;
    }

    private PureBitemporalOrderList getCachedPureBitemporalOrders()
    {
        return PureBitemporalOrderFinder.findMany(
            PureBitemporalOrderFinder.processingDate().equalsEdgePoint().and(
            PureBitemporalOrderFinder.businessDate().equalsEdgePoint()));
    }

    private PureType2DatedReadOnlyTypesAList getCachedPureType2DatedReadOnlyTypesA()
    {
        return PureType2DatedReadOnlyTypesAFinder.findMany(
            PureType2DatedReadOnlyTypesAFinder.processingDate().equalsEdgePoint().and(
            PureType2DatedReadOnlyTypesAFinder.businessDate().equalsEdgePoint()));
    }
}

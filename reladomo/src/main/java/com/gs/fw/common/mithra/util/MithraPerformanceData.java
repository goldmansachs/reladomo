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

package com.gs.fw.common.mithra.util;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;

public class MithraPerformanceData
{
    private MithraObjectPortal performanceDataOwner;
    private PerformanceDataPerOperation dataForFind = new PerformanceDataPerOperation();
    private PerformanceDataPerOperation dataForRefresh = new PerformanceDataPerOperation();
    private PerformanceDataPerOperation dataForInsert = new PerformanceDataPerOperation();
    private PerformanceDataPerOperation dataForUpdate = new PerformanceDataPerOperation();
    private PerformanceDataPerOperation dataForDelete = new PerformanceDataPerOperation();

    private boolean isRemote;
    private int queryCacheHits;
    private int objectCacheHits;

    public MithraPerformanceData(MithraObjectPortal performanceDataOwner)
    {
        this(performanceDataOwner, false);
    }

    public MithraPerformanceData(MithraObjectPortal performanceDataOwner, boolean remote)
    {
        this.performanceDataOwner = performanceDataOwner;
        isRemote = remote;
    }

    public boolean isRemote()
    {
        return isRemote;
    }

    public void incrementObjectCacheHits()
    {
        objectCacheHits++;
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).objectCacheHits++;
        }
    }

    public void incrementQueryCacheHits()
    {
        queryCacheHits++;
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).queryCacheHits++;
        }
    }

    public int getQueryCacheHits()
    {
        return queryCacheHits;
    }

    public int getObjectCacheHits()
    {
        return objectCacheHits;
    }

    public void recordTimeForFind(int objectsFound, long startTime)
    {
        long time = System.currentTimeMillis() - startTime;
        this.dataForFind.addTime(objectsFound, time);
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).dataForFind.addTime(objectsFound, time);
        }
    }

    public void recordTimeForRefresh(long startTime)
    {
        long time = System.currentTimeMillis() - startTime;
        this.dataForRefresh.addTime(1, time);
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).dataForRefresh.addTime(1, time);
        }
    }

    public void recordTimeForInsert(int objects, long startTime)
    {
        long time = System.currentTimeMillis() - startTime;
        this.dataForInsert.addTime(objects, time);
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).dataForInsert.addTime(objects, time);
        }
    }

    public void recordTimeForUpdate(int objects, long startTime)
    {
        long time = System.currentTimeMillis() - startTime;
        this.dataForUpdate.addTime(objects, time);
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).dataForUpdate.addTime(objects, time);
        }
    }

    public void recordTimeForDelete(int objects, long startTime)
    {
        long time = System.currentTimeMillis() - startTime;
        this.dataForDelete.addTime(objects, time);
        if(MithraManagerProvider.getMithraManager().canCaptureTransactionLevelPerformanceData())
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().getTransactionPerformanceDataFor(this.performanceDataOwner).dataForDelete.addTime(objects, time);
        }
    }

    public PerformanceDataPerOperation getDataForFind()
    {
        return dataForFind;
    }

    public PerformanceDataPerOperation getDataForRefresh()
    {
        return dataForRefresh;
    }

    public PerformanceDataPerOperation getDataForInsert()
    {
        return dataForInsert;
    }

    public PerformanceDataPerOperation getDataForUpdate()
    {
        return dataForUpdate;
    }

    public PerformanceDataPerOperation getDataForDelete()
    {
        return dataForDelete;
    }
}

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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.cache.StringIndex;
import com.gs.fw.common.mithra.util.StringPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterCacheUplink
{
    private static final Logger logger = LoggerFactory.getLogger(MasterCacheUplink.class);
    private static final AtomicInteger usedThreads = new AtomicInteger();
    private static final Long ZERO = 0L;

    private final String masterCacheId;
    private final MasterCacheService service;
    private volatile FastUnsafeOffHeapIntList masterToLocalStringMap = new FastUnsafeOffHeapIntList(10000, true);
    private int syncThreads;
    private long syncInterval;
    private List<MithraObjectPortal> objectPortals;
    private RefreshState nextRefresh;
    private final AtomicInteger activePortals = new AtomicInteger(0);
    private long lastSuccessfulRefresh = 0;
    private volatile boolean paused = false;
    private final Object pauseLock = new Object();

    public MasterCacheUplink(String masterCacheId, MasterCacheService service)
    {
        this.service = service;
        this.masterCacheId = masterCacheId;
    }

    public String getMasterCacheId()
    {
        return masterCacheId;
    }

    public long getLastSuccessfulRefresh()
    {
        return lastSuccessfulRefresh;
    }

    public long getSyncInterval()
    {
        return syncInterval;
    }

    public void setSyncInterval(long syncInterval)
    {
        this.syncInterval = syncInterval;
    }

    public void pause()
    {
        this.paused = true;
    }

    public void unPause()
    {
        synchronized (this.pauseLock)
        {
            this.paused = false;
            this.pauseLock.notifyAll();
        }
    }

    public int mapMasterStringRefToLocalRef(int masterStringRef)
    {
        if (masterStringRef >= masterToLocalStringMap.size())
        {
            refreshStringMap(masterStringRef);
        }
        if (masterStringRef >=  masterToLocalStringMap.size())
        {
            throwUnknownStringException(masterStringRef);
        }
        return masterToLocalStringMap.get(masterStringRef);
    }

    private void throwUnknownStringException(int masterStringRef)
    {
        throw new RuntimeException("Unknown master cache string ref: "+masterStringRef);
    }

    private synchronized void refreshStringMap(int masterStringRef)
    {
        if (masterStringRef >= masterToLocalStringMap.size())
        {
            syncStrings();
        }
    }

    private synchronized void syncStrings()
    {
        long start = System.currentTimeMillis();
        MasterRetrieveStringResult result = service.retrieveStrings(masterToLocalStringMap.size());
        int[] masterRefs = result.getMasterRefs();
        String[] masterStrings = result.getMasterStrings();
        StringPool.getInstance().ensureCapacity(masterRefs.length);
        if (masterRefs.length > this.masterToLocalStringMap.unusedCapacity())
        {
            FastUnsafeOffHeapIntList newMap = new FastUnsafeOffHeapIntList((int)((masterToLocalStringMap.size() + masterRefs.length)*1.1), true);
            newMap.addAll(masterToLocalStringMap);
            for(int i=0;i<masterRefs.length;i++)
            {
                int localStringRef = StringPool.getInstance().getOffHeapAddress(masterStrings[i]);
                assert masterStrings[i] == null || localStringRef != StringIndex.NULL_STRING;
                newMap.set(masterRefs[i], localStringRef);
            }
            this.masterToLocalStringMap = newMap;
        }
        else
        {
            for(int i=0;i<masterRefs.length;i++)
            {
                int localStringRef = StringPool.getInstance().getOffHeapAddress(masterStrings[i]);
                assert masterStrings[i] == null || localStringRef != StringIndex.NULL_STRING;
                masterToLocalStringMap.setWithFence(masterRefs[i], localStringRef);
            }
        }
        logger.info("String sync for "+masterCacheId+" processed "+masterRefs.length+" strings in "+(System.currentTimeMillis() - start)/1000.0+" seconds");
    }

    public synchronized void destroy()
    {
        logger.info("Master uplink for "+masterCacheId+" shutting down");
        if (this.masterToLocalStringMap != null)
        {
            this.masterToLocalStringMap.destroy();
            this.masterToLocalStringMap = null;
        }
    }

    public void setSyncThreads(int syncThreads)
    {
        this.syncThreads = syncThreads;
    }

    public int getSyncThreads()
    {
        return syncThreads;
    }

    public void startSyncAndWaitForInitialSync(List<MithraObjectPortal> objectPortals)
    {
        this.objectPortals = objectPortals;
        this.activePortals.set(this.objectPortals.size());
        MasterRetrieveInitialSyncSizeResult masterRetrieveInitialSyncSizeResult = this.service.retrieveInitialSyncSize();
        final Map<String,Long> nameToSizeMap = masterRetrieveInitialSyncSizeResult.getNameToSizeMap();
        Collections.sort(this.objectPortals, new Comparator<MithraObjectPortal>()
        {
            @Override
            public int compare(MithraObjectPortal o1, MithraObjectPortal o2)
            {
                Long size1 = nameToSizeMap.get(o1.getBusinessClassName());
                Long size2 = nameToSizeMap.get(o2.getBusinessClassName());
                if (size1 == null) size1 = ZERO;
                if (size2 == null) size2 = ZERO;
                return (size2 < size1 ? -1 : (size2 == size1 ? 0 : 1));
            }
        });
        RefreshState initialSyncState = new RefreshState(this.objectPortals.size(), System.currentTimeMillis());
        this.nextRefresh = initialSyncState;
        setUpThreadPool();
        initialSyncState.waitForRefreshToFinish();
    }

    private void setUpThreadPool()
    {
        for(int i=0;i<this.syncThreads;i++)
        {
            Thread t = new MasterCacheSynThread();
            t.start();
        }
    }

    public MasterSyncResult syncWithMasterCache(String businessClassName, long maxReplicatedPageVersion)
    {
        return this.service.syncWithMasterCache(businessClassName, maxReplicatedPageVersion);
    }

    private class MasterCacheSynThread extends Thread
    {
        private boolean done = false;
        private MithraObjectPortal lastPortal;

        private MasterCacheSynThread()
        {
            super("Mithra Master "+masterCacheId+" Cache Sync Thread "+usedThreads.incrementAndGet());
        }

        @Override
        public void run()
        {
            while(!done)
            {
                RefreshState refreshState = null;
                try
                {
                    refreshState = waitForRefreshToStart();
                    syncPortals(refreshState);
                }
                catch(Throwable t)
                {
                    if (refreshState != null)
                    {
                        refreshState.markRefreshFailed(t);
                        this.done = refreshState.decrementActiveThreadsAndWaitForRefreshToFinish();
                    }
                    String msg = lastPortal == null ? "" : " for "+lastPortal.getBusinessClassName();
                    logger.error("Refresh failed"+ msg, t);
                }
            }
            logger.info("Thread "+this.getName()+" exiting -- no more work to do");
        }

        private void syncPortals(RefreshState refreshState)
        {
            int portalIndex = refreshState.getNextPortal();
            if (portalIndex < 0)
            {
                syncStrings();
                portalIndex = refreshState.getNextPortal();
            }
            while(portalIndex < refreshState.activePortalsAtStartOfRefresh)
            {
                pauseIfNecessary();
                MithraObjectPortal portal = objectPortals.get(portalIndex);
                lastPortal = portal;
                if (portal != null)
                {
                    boolean destroyed = portal.syncWithMasterCache(MasterCacheUplink.this);
                    if (destroyed)
                    {
                        objectPortals.set(portalIndex, null);
                        activePortals.decrementAndGet();
                    }
                }
                portalIndex = refreshState.getNextPortal();
                lastPortal = null;
            }
            this.done = refreshState.decrementActiveThreadsAndWaitForRefreshToFinish();
        }

        private void pauseIfNecessary()
        {
            if (paused)
            {
                synchronized (pauseLock)
                {
                    while(paused)
                    {
                        try
                        {
                            pauseLock.wait();
                        }
                        catch (InterruptedException e)
                        {
                            //ignore
                        }
                    }
                }
            }
        }

        private RefreshState waitForRefreshToStart()
        {
            long time = System.currentTimeMillis();
            RefreshState refreshState = nextRefresh;
            while (time < refreshState.startTime)
            {
                try
                {
                    sleep(refreshState.startTime - time);
                }
                catch (InterruptedException e)
                {
                    //ignore -- todo: check done?
                }
                time = System.currentTimeMillis();
            }
            return refreshState;
        }
    }

    private class RefreshState
    {
        private final long startTime;
        private final int activePortalsAtStartOfRefresh;
        private AtomicInteger nextPortal = new AtomicInteger(-1);
        private int activeThreads = syncThreads;
        private long endTime;
        private Throwable failedThrowable;

        private RefreshState(int activePortalsAtStartOfRefresh, long startTime)
        {
            this.activePortalsAtStartOfRefresh = activePortalsAtStartOfRefresh;
            this.startTime = startTime;
        }

        private int getNextPortal()
        {
            return nextPortal.incrementAndGet() - 1;
        }

        public synchronized boolean decrementActiveThreadsAndWaitForRefreshToFinish()
        {
            activeThreads--;
            if (activeThreads > 0)
            {
                waitForRefreshToFinish();
            }
            else
            {
                // we are the last, we have to setup the next refresh
                if (activePortals.get() != 0)
                {
                    long end = System.currentTimeMillis();
                    long nextRefreshStart = this.startTime + syncInterval;
                    if (nextRefreshStart < end)
                    {
                        nextRefreshStart = end;
                    }
                    nextRefresh = new RefreshState(activePortals.get(), nextRefreshStart);
                    endTime = end;
                    if (this.failedThrowable == null)
                    {
                        lastSuccessfulRefresh = end;
                    }
                    this.notifyAll();
                    logger.info("Cache replication sync for "+masterCacheId+" finished in "+(endTime - startTime)/1000.0+" seconds");
                }
                else
                {
                    destroy();
                }
            }
            return activePortals.get() == 0;
        }

        public synchronized void waitForRefreshToFinish()
        {
            while(endTime == 0)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }

        public synchronized void markRefreshFailed(Throwable t)
        {
            this.failedThrowable = t;
        }
    }
}

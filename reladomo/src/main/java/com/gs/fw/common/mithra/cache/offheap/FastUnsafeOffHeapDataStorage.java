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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnStringAttribute;
import com.gs.fw.common.mithra.cache.AbstractDatedCache;
import com.gs.fw.common.mithra.cache.ParallelProcedure;
import com.gs.fw.common.mithra.cache.ReadWriteLock;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.ArrayBasedQueue;
import com.gs.fw.common.mithra.util.CpuBoundTask;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.FixedCountTaskFactory;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.MutableInteger;
import com.gs.fw.common.mithra.util.ThreadChunkSize;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.ObjectOutput;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

public class FastUnsafeOffHeapDataStorage implements OffHeapDataStorage
{
    private static final Logger logger = LoggerFactory.getLogger(FastUnsafeOffHeapDataStorage.class);
    public static final short PAGE_POWER_OF_TWO = 10;
    private static final int MAX_PAGES_TO_COPY_UNDER_LOCK = 10;
    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
//    private static MithraUnsafe.AuditedMemory UNSAFE = MithraUnsafe.getAuditedMemory();
    private static OffHeapFreeThread LATER_FREE_THREAD = new OffHeapFreeThread();
    private static long FREE_STACK_HEAD_OFFSET;

    private int current = 2;
    private int max;
    private final long dataSize;
    private long baseAddress;
    private long totalAllocated;
    private long maxIncreaseSize;
    private int totalFreed;
    private volatile int fence;
    private Object[] dataArray;
    private ReferenceQueue<WeakReferenceWithAddress> weakRefQueue;
    private volatile boolean destroyed;
    private volatile long freeStackHeadAndSize = 0;
    protected ReadWriteLock readWriteLock; // made protected only for test
    protected final FastUnsafeOffHeapLongList pageVersionList = new FastUnsafeOffHeapLongList(1000); // made protected only for test
    protected long currentPageVersion = 0; // made protected only for test
    private final String businessClassName;
    private long maxReplicatedPageVersion = 0;
    private OffHeapStringExtractor[] stringAttributes;
    private OffHeapExtractor[] pkAttributes;
    private Constructor dataConstructor;
    private RelatedFinder finder;

    static
    {
        Class<?> storageClass = FastUnsafeOffHeapDataStorage.class;
        try
        {
            FREE_STACK_HEAD_OFFSET = MithraUnsafe.getUnsafe().objectFieldOffset(storageClass.getDeclaredField("freeStackHeadAndSize"));
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException("could not get freeStackHead field", e);
        }

    }

    public FastUnsafeOffHeapDataStorage(int dataSize, String businessClassName, RelatedFinder finder)
    {
        LATER_FREE_THREAD.safeStart();

        this.dataSize = dataSize;
        this.totalAllocated = this.dataSize << PAGE_POWER_OF_TWO; // one page
        this.maxIncreaseSize = this.dataSize << (PAGE_POWER_OF_TWO+10); // 1024 pages
        baseAddress = UNSAFE.allocateMemory(totalAllocated);
        UNSAFE.setMemory(baseAddress, totalAllocated, (byte) 0);
        UNSAFE.putByte(baseAddress, AbstractDatedCache.REMOVED_VERSION); // we don't store anything in address 0 or 1
        UNSAFE.putByte(baseAddress + dataSize, AbstractDatedCache.REMOVED_VERSION); // we don't store anything in address 0 or 1
        computeMax();
        this.dataArray = new Object[max+2];
        weakRefQueue = new ReferenceQueue();
        this.businessClassName = businessClassName;
        this.finder = finder;
        if (finder != null)
        {
            populateStringAttributes(finder);
            createDataConstructor(finder);
            populatePkAttributes(finder);
        }
    }

    private void createDataConstructor(RelatedFinder finder)
    {
        String finderClassName = finder.getFinderClassName();
        String qualifiedBusinessClassName = finderClassName.substring(0, finderClassName.length() - "Finder".length());
        String businessClassName = qualifiedBusinessClassName.substring(qualifiedBusinessClassName.lastIndexOf('.')+1);
        String dataClassName = qualifiedBusinessClassName +"Data$"+businessClassName+"DataOffHeap";
        try
        {
            this.dataConstructor = Class.forName(dataClassName).getConstructor(MutableInteger.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not find constructor for "+dataClassName, e);
        }
    }

    private void populateStringAttributes(RelatedFinder finder)
    {
        Attribute[] persistentAttributes = finder.getPersistentAttributes();
        List<OffHeapStringExtractor> tmp = MithraFastList.newList();
        Attribute sourceAttribute = finder.getSourceAttribute();
        if (sourceAttribute != null && sourceAttribute instanceof SingleColumnStringAttribute)
        {
            tmp.add((OffHeapStringExtractor) sourceAttribute.zCreateOffHeapExtractor());
        }
        for(Attribute a: persistentAttributes)
        {
            if (a instanceof SingleColumnStringAttribute)
            {
                tmp.add((OffHeapStringExtractor) a.zCreateOffHeapExtractor());
            }
        }
        stringAttributes = tmp.toArray(new OffHeapStringExtractor[tmp.size()]);
    }

    private void populatePkAttributes(RelatedFinder finder)
    {
        List<OffHeapExtractor> pkExtractorList = FastList.newList();
        Attribute[] pkAttributesWithoutProcessingFrom = finder.getPrimaryKeyAttributes();
        for(Attribute a: pkAttributesWithoutProcessingFrom)
        {
            pkExtractorList.add(a.zCreateOffHeapExtractor());
        }
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        for(AsOfAttribute a: asOfAttributes)
        {
            pkExtractorList.add(a.getFromAttribute().zCreateOffHeapExtractor());
        }
        this.pkAttributes = pkExtractorList.toArray(new OffHeapExtractor[pkExtractorList.size()]);
    }

    private MithraOffHeapDataObject createData(MutableInteger[] index)
    {
        try
        {
            return (MithraOffHeapDataObject) dataConstructor.newInstance(index);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not instantiate data", e);
        }
    }

    public FastUnsafeOffHeapDataStorage(int dataSize, ReadWriteLock lock)
    {
        this(dataSize, "", null);
        this.readWriteLock = lock;
    }

    @Override
    public void setReadWriteLock(ReadWriteLock cacheLock)
    {
        this.readWriteLock = cacheLock;
    }

    @Override
    public synchronized boolean syncWithMasterCache(MasterCacheUplink uplink, OffHeapSyncableCache cache)
    {
        if (destroyed)
        {
            return true;
        }
        cache.setReplicationMode();
        MasterSyncResult result = null;
        try
        {
            SyncLog syncLog = new SyncLog(this.businessClassName);
            syncLog.setInitialMaxPageVersion(this.maxReplicatedPageVersion);
            result = uplink.syncWithMasterCache(this.businessClassName, maxReplicatedPageVersion);
            syncLog.logMasterResultReceived(result);
            if (destroyed)
            {
                logger.info("Stopping sync for "+this.businessClassName);
                return true;
            }
            result.fixUpStringReferences(this.stringAttributes, uplink, this.dataSize);
            syncLog.logStringsFixedUp();
            processIncomingSyncResult(result, cache, syncLog);
            syncLog.logSyncFinished();
            this.finder.getMithraObjectPortal().setLatestRefreshTime(result.getLastMasterRefreshTime());
            if (!result.isEmpty())
            {
                this.finder.getMithraObjectPortal().incrementClassUpdateCount();
            }
            syncLog.printLog();
            return false;
        }
        finally
        {
            if (result != null)
            {
                result.destroy();
            }
        }
    }

    private void processIncomingSyncResult(MasterSyncResult syncResult, OffHeapSyncableCache cache, SyncLog syncLog)
    {
        if (syncResult.getBuffers().isEmpty())
        {
            return;
        }
        int currentCopy = this.current;
        if (currentCopy == 2)
        {
            processInitialSync(syncResult, cache, syncLog);
        }
        else
        {
            IntLongHashMap pageLocationMap = syncResult.getPageLocationMap();
            FastList<FastUnsafeOffHeapPageBuffer> buffers = syncResult.getBuffers();
            int currentPage = this.current >> PAGE_POWER_OF_TWO;
            int maxPage = currentPage;

            for (IntIterator intIterator = pageLocationMap.keySet().intIterator(); intIterator.hasNext(); )
            {
                int pageIndex = intIterator.next();
                if (pageIndex > maxPage)
                {
                    maxPage = pageIndex;
                }
            }
            if (maxPage >= (max >> PAGE_POWER_OF_TWO))
            {
                processDataViaCopy(syncResult, maxPage, cache, syncLog);
            }
            else
            {
                processDataInPlace(syncResult, maxPage, cache, syncLog);
            }
        }
        this.maxReplicatedPageVersion = syncResult.getMaxReplicatedVersion();
        syncLog.setFinalMaxPageVersion(this.maxReplicatedPageVersion);
    }

    private void processInitialSync(MasterSyncResult syncResult, OffHeapSyncableCache cache, SyncLog syncLog)
    {
        syncLog.setInitialSync();
        FastList<FastUnsafeOffHeapPageBuffer> buffers = syncResult.getBuffers();
        if (buffers.size() == 1)
        {
            processInitialSyncWithoutCopy(cache, buffers.get(0));
        }
        else
        {
            processInitialSyncWithCopy(syncResult, cache, buffers);
        }
    }

    private void processInitialSyncWithCopy(MasterSyncResult syncResult, OffHeapSyncableCache cache, FastList<FastUnsafeOffHeapPageBuffer> buffers)
    {
        int pages = 0;
        for(int i=0;i<buffers.size();i++)
        {
            pages += buffers.get(i).getUsedPages();
        }
        IntLongHashMap pageLocationMap = syncResult.getPageLocationMap();
        checkBufferPages(buffers, pages, pageLocationMap);
        long oldBase = this.baseAddress;
        totalAllocated = pages*this.getPageSize();
        this.baseAddress = UNSAFE.allocateMemory(totalAllocated);
        computeMax();
        int dataCount = pages << PAGE_POWER_OF_TWO;
        dataArray = new Object[dataCount];
        long pageSize = this.getPageSize();
        long copyAddress = this.baseAddress;
        this.pageVersionList.clear();
        for(int i=0;i<buffers.size();i++)
        {
            FastUnsafeOffHeapPageBuffer buffer = buffers.get(i);
            int pagesToCopy = buffer.getUsedPages();
            assertedCopyMemory(buffer.getBaseAddress(), copyAddress, pagesToCopy*pageSize, buffer.getBaseAddress(), buffer.getAllocatedLength(), this.baseAddress, this.totalAllocated);
            copyAddress += pagesToCopy*pageSize;
            this.pageVersionList.addAll(buffer.getMasterPageVersions());
        }
        MutableInteger[] constructorArg = createDataConstructorArg();
        this.readWriteLock.acquireWriteLock();
        try
        {
            for(int dataIndex=0;dataIndex<dataCount;dataIndex++)
            {
                int page = dataIndex >> PAGE_POWER_OF_TWO;
                long pageFromSyncResult = pageLocationMap.get(page);
                int pageBufferLocation = syncResult.getPageBufferLocation(pageFromSyncResult);
                int bufferPageIndex = syncResult.getBufferPageIndex(pageFromSyncResult);
                FastUnsafeOffHeapPageBuffer buffer = buffers.get(pageBufferLocation);
                if (buffer.getUsedData().get(bufferPageIndex, dataIndex & ((1 << PAGE_POWER_OF_TWO)-1)))
                {
                    constructDataAndAddToCache(cache, constructorArg, dataIndex);
                }
            }
            this.current = dataCount - 1;
        }
        finally
        {
            this.readWriteLock.release();
        }
        LATER_FREE_THREAD.queue(oldBase);
    }

    private void checkBufferPages(FastList<FastUnsafeOffHeapPageBuffer> buffers, int pages, IntLongHashMap pageLocationMap)
    {
        for(int page=0;page<pages;page++)
        {
            long pageFromSyncResult = pageLocationMap.get(page);
            if (pageFromSyncResult == 0)
            {
                logger.error("Missing page "+page+" in initial sync");
                String msg = "Buffers "+buffers.size()+"\n";
                for(int i=0;i<buffers.size();i++)
                {
                    String bufMsg = "buffer "+i+": ";
                    FastUnsafeOffHeapIntList pageIndicies = buffers.get(i).getMasterPageIndicies();
                    for(int j=0;j<pageIndicies.size();j++)
                    {
                        bufMsg+= ", "+pageIndicies.get(j);
                    }
                    msg += bufMsg+"\n";
                }
                logger.error(msg);
                throw new RuntimeException("Unexpected buffer pages. see above");
            }
        }
        int copied = 0;
        for(int i=0;i<buffers.size();i++)
        {
            FastUnsafeOffHeapPageBuffer buffer = buffers.get(i);
            int pagesToCopy = buffer.getUsedPages();
            assert copied == buffer.getMasterPageIndicies().get(0);
            copied += pagesToCopy;
        }
    }

    private MutableInteger[] createDataConstructorArg()
    {
        MutableInteger[] constructorArg = new MutableInteger[1];
        constructorArg[0] = new MutableInteger();
        return constructorArg;
    }

    private void processInitialSyncWithoutCopy(OffHeapSyncableCache cache, FastUnsafeOffHeapPageBuffer buffer)
    {
        long oldBase = this.baseAddress;
        this.baseAddress = buffer.getBaseAddress();
        totalAllocated = buffer.getAllocatedPages()*this.getPageSize();
        computeMax();
        int dataCount = buffer.getUsedPages() << PAGE_POWER_OF_TWO;
        dataArray = new Object[dataCount];
        MutableInteger[] constructorArg = createDataConstructorArg();
        this.readWriteLock.acquireWriteLock();
        try
        {
            for(int localDataIndex=0;localDataIndex<dataCount;localDataIndex++)
            {
                if (buffer.getUsedData().get(localDataIndex))
                {
                    constructDataAndAddToCache(cache, constructorArg, localDataIndex);
                }
            }
            this.pageVersionList.clearAndCopy(buffer.getMasterPageVersions());
            this.current = dataCount - 1;
        }
        finally
        {
            this.readWriteLock.release();
        }
        buffer.destroyWithoutBufferDeallocation();
        LATER_FREE_THREAD.queue(oldBase);
    }

    private void processDataViaCopy(MasterSyncResult syncResult, int maxPage, OffHeapSyncableCache cache, SyncLog syncLog)
    {
        syncLog.setUpdateViaCopy();
        int currentMaxPage = this.current >> PAGE_POWER_OF_TWO;
        FastList<FastUnsafeOffHeapPageBuffer> buffers = syncResult.getBuffers();
        FastUnsafeOffHeapIntList toInsert = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toRemove = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toUpdate = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toNukeAndInsert = new FastUnsafeOffHeapIntList(1000);

        try
        {
            bucketAllIncomingData(currentMaxPage, buffers, toInsert, toRemove, toUpdate, toNukeAndInsert, syncLog);
            long newBase = copyExistingAndIncomingPagesIntoNew(syncResult, maxPage);
            MutableInteger[] constructorArg = createDataConstructorArg();
            this.readWriteLock.acquireWriteLock();
            try
            {
                for(int i=0;i<toRemove.size();i++)
                {
                    int toRemoveDataIndex = toRemove.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    cache.syncDataRemove(data);
                    this.free(toRemoveDataIndex);
                }
                for(int i=0;i<toUpdate.size();i++)
                {
                    int toRemoveDataIndex = toUpdate.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    cache.syncDataRemove(data);
                }
                for(int i=0;i<toNukeAndInsert.size();i++)
                {
                    int toRemoveDataIndex = toNukeAndInsert.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    if (data != null)
                    {
                        this.dataArray[toRemoveDataIndex] = null;
                        if (data instanceof WeakReferenceWithAddress)
                        {
                            data = ((WeakReferenceWithAddress)data).get();
                            if (data != null)
                            {
                                ((MithraOffHeapDataObject)data).zSetOffset(0);
                            }
                        }
                        else
                        {
                            cache.syncDataRemove(data);
                            ((MithraOffHeapDataObject)data).zSetOffset(0);
                        }
                    }
                }
                long oldBase = this.baseAddress;
                this.baseAddress = newBase;
                computeMax();
                dataArray = Arrays.copyOf(dataArray, max + 2);
                fence++; // ensure baseAddress is visible in other threads
                LATER_FREE_THREAD.queue(oldBase);
                IntLongHashMap pageLocationMap = syncResult.getPageLocationMap();
                updateCacheAfterRemovalAndBufferCopy(syncResult, maxPage, cache, currentMaxPage, buffers, toInsert, toUpdate, toNukeAndInsert, pageLocationMap, constructorArg);

                updateLocalPageVersions(buffers);
            }
            finally
            {
                this.readWriteLock.release();
            }
        }
        finally
        {
            toInsert.destroy();
            toRemove.destroy();
            toUpdate.destroy();
            toNukeAndInsert.destroy();
        }
    }

    private void processDataInPlace(MasterSyncResult syncResult, int maxPage, OffHeapSyncableCache cache, SyncLog syncLog)
    {
        syncLog.setProcessInPlace();
        int currentMaxPage = this.current >> PAGE_POWER_OF_TWO;
        FastList<FastUnsafeOffHeapPageBuffer> buffers = syncResult.getBuffers();
        FastUnsafeOffHeapIntList toInsert = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toRemove = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toUpdate = new FastUnsafeOffHeapIntList(1000);
        FastUnsafeOffHeapIntList toNukeAndInsert = new FastUnsafeOffHeapIntList(1000);
        IntLongHashMap pageLocationMap = syncResult.getPageLocationMap();

        try
        {
            bucketAllIncomingData(currentMaxPage, buffers, toInsert, toRemove, toUpdate, toNukeAndInsert, syncLog);
            copyPagesAfterCurrent(syncResult, maxPage, getPageSize(), this.baseAddress, pageLocationMap, buffers, currentMaxPage, this.totalAllocated);
            MutableInteger[] constructorArg = createDataConstructorArg();
            this.readWriteLock.acquireWriteLock();
            try
            {
                for(int i=0;i<toInsert.size();i++)
                {
                    int toInsertDataIndex = toInsert.get(i);
                    copyDataFromBuffers(syncResult, buffers, pageLocationMap, toInsertDataIndex);
                }
                for(int i=0;i<toRemove.size();i++)
                {
                    int toRemoveDataIndex = toRemove.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    cache.syncDataRemove(data);
                    this.free(toRemoveDataIndex);
                }
                for(int i=0;i<toUpdate.size();i++)
                {
                    int toRemoveDataIndex = toUpdate.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    cache.syncDataRemove(data);
                    copyDataFromBuffers(syncResult, buffers, pageLocationMap, toRemoveDataIndex);
                }
                for(int i=0;i<toNukeAndInsert.size();i++)
                {
                    int toRemoveDataIndex = toNukeAndInsert.get(i);
                    Object data = this.dataArray[toRemoveDataIndex];
                    if (data != null)
                    {
                        this.dataArray[toRemoveDataIndex] = null;
                        if (data instanceof WeakReferenceWithAddress)
                        {
                            data = ((WeakReferenceWithAddress)data).get();
                            if (data != null)
                            {
                                ((MithraOffHeapDataObject)data).zSetOffset(0);
                            }
                        }
                        else
                        {
                            cache.syncDataRemove(data);
                            ((MithraOffHeapDataObject)data).zSetOffset(0);
                        }
                    }
                    copyDataFromBuffers(syncResult, buffers, pageLocationMap, toRemoveDataIndex);
                }
                updateCacheAfterRemovalAndBufferCopy(syncResult, maxPage, cache, currentMaxPage, buffers, toInsert, toUpdate, toNukeAndInsert, pageLocationMap, constructorArg);

                updateLocalPageVersions(buffers);
            }
            finally
            {
                this.readWriteLock.release();
            }
        }
        finally
        {
            toInsert.destroy();
            toRemove.destroy();
            toUpdate.destroy();
            toNukeAndInsert.destroy();
        }
    }

    private void updateLocalPageVersions(List<FastUnsafeOffHeapPageBuffer> buffers)
    {
        for (int i = 0; i < buffers.size(); i++)
        {
            FastUnsafeOffHeapPageBuffer buffer = buffers.get(i);
            FastUnsafeOffHeapIntList masterPageIndicies = buffer.getMasterPageIndicies();
            FastUnsafeOffHeapLongList masterPageVersions = buffer.getMasterPageVersions();

            for (int bufferPageIndex = 0; bufferPageIndex < masterPageVersions.size(); bufferPageIndex++)
            {
                int masterPageIndex = masterPageIndicies.get(bufferPageIndex);
                long masterPageVersion = masterPageVersions.get(bufferPageIndex);

                this.pageVersionList.set(masterPageIndex, masterPageVersion);
            }
        }
    }

    private void updateCacheAfterRemovalAndBufferCopy(MasterSyncResult syncResult, int maxPage, OffHeapSyncableCache cache,
            int currentMaxPage, FastList<FastUnsafeOffHeapPageBuffer> buffers, FastUnsafeOffHeapIntList toInsert,
            FastUnsafeOffHeapIntList toUpdate, FastUnsafeOffHeapIntList toNukeAndInsert,
            IntLongHashMap pageLocationMap, MutableInteger[] constructorArg)
    {
        for(int i=0;i<toUpdate.size();i++)
        {
            int toAdd = toUpdate.get(i);
            Object data = this.dataArray[toAdd];
            cache.syncDataAdd(data);
        }
        for(int i=0;i<toNukeAndInsert.size();i++)
        {
            int toAdd = toNukeAndInsert.get(i);
            constructDataAndAddToCache(cache, constructorArg, toAdd);
        }
        for(int i=0;i<toInsert.size();i++)
        {
            int toAdd = toInsert.get(i);
            constructDataAndAddToCache(cache, constructorArg, toAdd);
        }
        for(int page=currentMaxPage+1;page <= maxPage;page++)
        {
            long pageFromSyncResult = pageLocationMap.get(page);
            int pageBufferLocation = syncResult.getPageBufferLocation(pageFromSyncResult);
            int bufferPageIndex = syncResult.getBufferPageIndex(pageFromSyncResult);
            FastUnsafeOffHeapPageBuffer buffer = buffers.get(pageBufferLocation);
            FastUnSafeOffHeapBitSet usedData = buffer.getUsedData();
            for(int pageDataIndex=0;pageDataIndex < (1 << PAGE_POWER_OF_TWO); pageDataIndex++)
            {
                if (usedData.get(bufferPageIndex, pageDataIndex))
                {
                    constructDataAndAddToCache(cache, constructorArg, (page << PAGE_POWER_OF_TWO) + pageDataIndex);
                }
            }
        }
    }

    private void copyDataFromBuffers(MasterSyncResult syncResult, FastList<FastUnsafeOffHeapPageBuffer> buffers, IntLongHashMap pageLocationMap, int toCopyDataIndex)
    {
        int page = toCopyDataIndex >> PAGE_POWER_OF_TWO;
        int pageDataIndex = toCopyDataIndex & ((1 << PAGE_POWER_OF_TWO) - 1);
        long pageFromSyncResult = pageLocationMap.get(page);
        int pageBufferLocation = syncResult.getPageBufferLocation(pageFromSyncResult);
        int bufferPageIndex = syncResult.getBufferPageIndex(pageFromSyncResult);
        FastUnsafeOffHeapPageBuffer buffer = buffers.get(pageBufferLocation);
        long src = buffer.getPageStartLocation(bufferPageIndex) + pageDataIndex * dataSize;
        long dest = computeAddress(toCopyDataIndex, 0);
        assertedCopyMemory(src + 1, dest + 1, dataSize - 1, buffer.getBaseAddress(), buffer.getAllocatedLength(), this.baseAddress, this.totalAllocated);
        fence++;
        UNSAFE.putByte(dest, UNSAFE.getByte(src)); // we copy the dataVersion field last
    }

    private void bucketAllIncomingData(int currentMaxPage, FastList<FastUnsafeOffHeapPageBuffer> buffers, FastUnsafeOffHeapIntList toInsert,
            FastUnsafeOffHeapIntList toRemove, FastUnsafeOffHeapIntList toUpdate, FastUnsafeOffHeapIntList toNukeAndInsert, SyncLog syncLog)
    {
        for (int i = 0; i < buffers.size(); i++)
        {
            FastUnsafeOffHeapPageBuffer buffer = buffers.get(i);
            FastUnsafeOffHeapIntList masterPageIndicies = buffer.getMasterPageIndicies();
            for (int bufferPageIndex = 0; bufferPageIndex < masterPageIndicies.size(); bufferPageIndex++)
            {
                int page = masterPageIndicies.get(bufferPageIndex);
                if (page <= currentMaxPage)
                {
                    for (int pageDataIndex = 0; pageDataIndex < (1 << PAGE_POWER_OF_TWO); pageDataIndex++)
                    {
                        bucketIncomingData(toInsert, toRemove, toUpdate, toNukeAndInsert, buffer, bufferPageIndex, page, pageDataIndex);
                    }
                }
            }
        }
        syncLog.setToInsertInExistingPages(toInsert.size());
        syncLog.setToUpdateExisting(toUpdate.size());
        syncLog.setToRemoveExisting(toRemove.size());
        syncLog.setToNukeAndInsert(toNukeAndInsert.size());
    }

    private void constructDataAndAddToCache(OffHeapSyncableCache cache, MutableInteger[] constructorArg, int toAdd)
    {
        constructorArg[0].replace(toAdd);
        this.dataArray[toAdd] = createData(constructorArg);
        cache.syncDataAdd(this.dataArray[toAdd]);
        if (this.current < toAdd)
        {
            this.current = toAdd;
        }
    }

    private void bucketIncomingData(FastUnsafeOffHeapIntList toInsert, FastUnsafeOffHeapIntList toRemove,
            FastUnsafeOffHeapIntList toUpdate, FastUnsafeOffHeapIntList toNukeAndInsert,
            FastUnsafeOffHeapPageBuffer buffer, int bufferPageIndex, int page, int pageDataIndex)
    {
        int localDataIndex = (page <<PAGE_POWER_OF_TWO)+pageDataIndex;
        boolean bufferDataIsUsed = buffer.getUsedData().get(bufferPageIndex, pageDataIndex);
        Object localData = this.dataArray[localDataIndex];
        if (bufferDataIsUsed)
        {
            if (localData == null)
            {
                toInsert.add(localDataIndex);
            }
            else if (localData instanceof WeakReferenceWithAddress)
            {
                localData = ((WeakReferenceWithAddress) localData).get();
                if (localData == null)
                {
                    toInsert.add(localDataIndex);
                }
                else
                {
                    //treat this as a pk mismatch
                    toNukeAndInsert.add(localDataIndex);
                }
            }
            else
            {
                // compare
                if (isByteWiseDifferent(localDataIndex, buffer, bufferPageIndex, pageDataIndex))
                {
                    if (hasTheSamePk(localDataIndex, buffer, bufferPageIndex, pageDataIndex))
                    {
                        toUpdate.add(localDataIndex);
                    }
                    else
                    {
                        toNukeAndInsert.add(localDataIndex);
                    }
                }
            }
        }
        else
        {
            if (localData == null)
            {
                //nothing to do
            }
            else if (localData instanceof WeakReferenceWithAddress)
            {
                localData = ((WeakReferenceWithAddress) localData).get();
                if (localData == null)
                {
                    // do nothing
                }
                else
                {
                    // overwrite the buffer memory from local copy
                    assertedCopyMemory(computeAddress(localDataIndex, 0), getBufferDataStartAddress(buffer, bufferPageIndex, pageDataIndex), dataSize,
                            this.baseAddress, this.totalAllocated, buffer.getBaseAddress(), buffer.getAllocatedLength());
                }
            }
            else
            {
                toRemove.add(localDataIndex);
            }
        }
    }

    private void assertedCopyMemory(long src, long dest, long copySize, long srcBase, long srcLength, long destBase, long destLength)
    {
        assert copySize >= 0;
        assert src >= srcBase;
        assert src + copySize <= srcBase + srcLength;
        assert dest >= destBase;
        assert dest + copySize <= destBase + destLength;
        UNSAFE.copyMemory(src, dest, copySize);
    }

    private boolean hasTheSamePk(int localDataIndex, FastUnsafeOffHeapPageBuffer buffer, int bufferPageIndex, int pageDataIndex)
    {
        for(OffHeapExtractor e: pkAttributes)
        {
            if (!e.valueEquals(this, localDataIndex, getBufferDataStartAddress(buffer, bufferPageIndex, pageDataIndex)))
            {
                return false;
            }
        }
        return true;
    }

    private long getBufferDataStartAddress(FastUnsafeOffHeapPageBuffer buffer, int bufferPageIndex, int pageDataIndex)
    {
        return buffer.getBaseAddress() + ((bufferPageIndex << PAGE_POWER_OF_TWO) + pageDataIndex) * dataSize;
    }

    private boolean isByteWiseDifferent(int localDataIndex, FastUnsafeOffHeapPageBuffer buffer, int bufferPageIndex, int pageDataIndex)
    {
        int numLongs = (int)(dataSize >> 3);
        int remainingBytes = (int) (dataSize & 7);
        long localPointer = computeAddress(localDataIndex, 0);
        long bufferPointer = getBufferDataStartAddress(buffer, bufferPageIndex, pageDataIndex);
        for(int i=0;i<numLongs;i++)
        {
            if (UNSAFE.getLong(localPointer) != UNSAFE.getLong(bufferPointer))
            {
                return true;
            }
            localPointer += 8;
            bufferPointer += 8;
        }
        for(int i=0;i<remainingBytes;i++)
        {
            if (UNSAFE.getByte(localPointer + i) != UNSAFE.getByte(bufferPointer + i))
            {
                return true;
            }
        }
        return false;
    }

    private long copyExistingAndIncomingPagesIntoNew(MasterSyncResult syncResult, int maxPage)
    {
        long pageSize = getPageSize();
        long newSize = (int)((maxPage+1)*1.1) * pageSize;
        long newBase = UNSAFE.allocateMemory(newSize);

        IntLongHashMap pageLocationMap = syncResult.getPageLocationMap();
        FastList<FastUnsafeOffHeapPageBuffer> buffers = syncResult.getBuffers();
        int currentPage = this.current >> PAGE_POWER_OF_TWO;
        for(int page=0;page<=currentPage;page++)
        {
            long pageFromSyncResult = pageLocationMap.get(page);
            if (pageFromSyncResult == 0)
            {
                assertedCopyMemory(this.baseAddress + page * pageSize, newBase + page*pageSize, pageSize, this.baseAddress, this.totalAllocated, newBase, newSize);
            }
            else
            {
                copyBufferPage(syncResult, pageSize, newBase, buffers, page, pageFromSyncResult, newSize);
            }
        }
        copyPagesAfterCurrent(syncResult, maxPage, pageSize, newBase, pageLocationMap, buffers, currentPage, newSize);
        long remainder = newSize - (maxPage + 1) * pageSize;
        if (remainder > 0)
        {
            UNSAFE.setMemory(newBase + (maxPage+1)*pageSize, remainder, (byte) 0);
        }
        totalAllocated = newSize;
        return newBase;
    }

    private void copyPagesAfterCurrent(MasterSyncResult syncResult, int maxPage, long pageSize, long newBase,
            IntLongHashMap pageLocationMap, FastList<FastUnsafeOffHeapPageBuffer> buffers, int currentPage, long newSize)
    {
        for(int page=currentPage+1;page<=maxPage;page++)
        {
            long pageFromSyncResult = pageLocationMap.get(page);
            if (pageFromSyncResult == 0)
            {
                throw new RuntimeException("missing page after current page "+page);
            }
            else
            {
                copyBufferPage(syncResult, pageSize, newBase, buffers, page, pageFromSyncResult, newSize);
            }
        }

        int newCurrent = ((maxPage + 1) << PAGE_POWER_OF_TWO) - 1;
        if (newCurrent > this.current)
        {
            this.current = newCurrent;
        }
    }

    private void copyBufferPage(MasterSyncResult syncResult, long pageSize, long newBase, FastList<FastUnsafeOffHeapPageBuffer> buffers,
            int destinationPageIndex, long pageFromSyncResult, long newSize)
    {
        int pageBufferLocation = syncResult.getPageBufferLocation(pageFromSyncResult);
        int bufferPageIndex = syncResult.getBufferPageIndex(pageFromSyncResult);
        FastUnsafeOffHeapPageBuffer buffer = buffers.get(pageBufferLocation);
        assertedCopyMemory(buffer.getPageStartLocation(bufferPageIndex), newBase + destinationPageIndex * pageSize, pageSize,
                buffer.getBaseAddress(), buffer.getAllocatedLength(), newBase, newSize);
    }

    @Override
    public void markDataDirty(int dataOffset)
    {
        this.pageVersionList.set(dataOffset >> PAGE_POWER_OF_TWO, 0);
    }

    @Override
    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion)
    {
        return new MasterSyncResult(this, maxReplicatedPageVersion);
    }

    private void computeMax()
    {
        max = (int) (this.totalAllocated/dataSize);
    }

    private int getStackHead(long headAndSize)
    {
        return (int) (headAndSize & 0x7FFFFFF);
    }

    private int getStackSize(long headAndSize)
    {
        return (int) ((headAndSize >> 32) & 0x7FFFFFF);
    }

    private boolean casFreeStackHead(long expected, int newHead, int sizeIncrement)
    {
        long newValue = newHead | (((long)(getStackSize(expected) + sizeIncrement)) << 32);
        return UNSAFE.compareAndSwapLong(this, FREE_STACK_HEAD_OFFSET, expected, newValue);
    }

    @Override
    public int allocate(MithraOffHeapDataObject data)
    {
        while(true)
        {
            long curStackHead = freeStackHeadAndSize;
            int stackHead = getStackHead(curStackHead);
            if (stackHead != 0)
            {
                if (casFreeStackHead(curStackHead, getInt(stackHead, 0), -1))
                {
                    totalFreed--;
                    UNSAFE.setMemory(computeAddress(stackHead, 0), dataSize, (byte) 0);
                    dataArray[stackHead] = data;
                    markDataDirty(stackHead);
                    return stackHead;
                }
            }
            else
            {
                if (current+1 == max)
                {
                    reallocate();
                }
                dataArray[++current] = data;
                markDataDirty(current);
                return current;
            }
        }
    }

    private void reallocate()
    {
        long newSize = this.totalAllocated;
        if (totalAllocated < maxIncreaseSize)
        {
            newSize <<= 1;
        }
        else
        {
            newSize += maxIncreaseSize;
        }
        reallocate(newSize);
    }

    private void reallocate(long newSize)
    {
        assert newSize % getPageSize() == 0;
        long newBase = UNSAFE.allocateMemory(newSize);
        assert newSize >= totalAllocated;
        UNSAFE.copyMemory(this.baseAddress, newBase, this.totalAllocated);
        UNSAFE.setMemory(newBase + totalAllocated, newSize - totalAllocated, (byte) 0);
        long oldBase = this.baseAddress;
        this.baseAddress = newBase;
        totalAllocated = newSize;
        computeMax();
        dataArray = Arrays.copyOf(dataArray, max + 2);
        fence++; // ensure baseAddress is visible in other threads
        LATER_FREE_THREAD.queue(oldBase);
    }

    @Override
    public void free(int dataOffset)
    {
        WeakReferenceWithAddress weak = new WeakReferenceWithAddress(dataArray[dataOffset], weakRefQueue, dataOffset);
        dataArray[dataOffset] = weak;
        totalFreed++;
        markDataDirty(dataOffset);
    }

    @Override
    public boolean isBooleanNull(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getByte(computeAddress(dataOffset, fieldOffset)) == 2;
    }

    @Override
    public void setBooleanNull(int dataOffset, int fieldOffset)
    {
        UNSAFE.putByte(computeAddress(dataOffset, fieldOffset), (byte) 2);
    }

    @Override
    public void destroy()
    {
        if (!destroyed)
        {
            UNSAFE.freeMemory(this.baseAddress);
            destroyed = true;
            this.baseAddress = -(1L << 40); // about a terabyte
            this.dataArray = null;
            this.pageVersionList.destroy();
        }
    }

    @Override
    public boolean forAll(DoUntilProcedure procedure)
    {
        boolean done = false;
        int length = current + 1;
        for (int i = 2; i < length && !done; i++)
        {
            Object o = dataArray[i];
            if (o != null && !(o instanceof WeakReferenceWithAddress))
            {
                done = procedure.execute(o);
            }
        }
        return done;
    }

    @Override
    public void forAllInParallel(final ParallelProcedure procedure)
    {
        MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
        int length = current + 1;
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(pool.getThreads(), length, 1);
        final ArrayBasedQueue queue = new ArrayBasedQueue(length, threadChunkSize.getChunkSize());
        int threads = threadChunkSize.getThreads();
        procedure.setThreads(threads, length /threads);
        CpuBoundTask[] tasks = new CpuBoundTask[threads];
        for(int i=0;i<threads;i++)
        {
            final int thread = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    ArrayBasedQueue.Segment segment = queue.borrow(null);
                    while(segment != null)
                    {
                        for (int i = segment.getStart(); i < segment.getEnd(); i++)
                        {
                            Object o = dataArray[i];
                            if (o != null && !(o instanceof WeakReferenceWithAddress))
                            {
                                procedure.execute(o, thread);
                            }
                        }
                        segment = queue.borrow(segment);
                    }
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();

    }

    @Override
    public void clear()
    {
        int length = current + 1;
        for (int i = 2; i < length; i++)
        {
            Object o = dataArray[i];
            if (o != null && !(o instanceof WeakReferenceWithAddress))
            {
                free(i);
            }
        }
        this.pageVersionList.clear();
    }

    @Override
    public void ensureExtraCapacity(int size)
    {
        if (freeCapacity() < size)
        {
            int needExtra = size - freeCapacity() + 10;
            int pageMask = (1 << PAGE_POWER_OF_TWO) - 1;
            if ((needExtra & pageMask) != 0)
            {
                needExtra = (needExtra & ~pageMask) + (1 << PAGE_POWER_OF_TWO);
            }
            long target = this.totalAllocated + needExtra *dataSize;
            reallocate(target);
        }
    }

    public int freeCapacity()
    {
        return max - current + getStackSize(freeStackHeadAndSize);
    }

    public int zGetStackSizeForTest()
    {
        return getStackSize(freeStackHeadAndSize);
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {
        logger.debug("Class "+className+" data size "+this.dataSize+" bytes allocated: "+totalAllocated
                +" live objects "+(current - totalFreed - 2)
                + " used bytes "+(current * dataSize)+" unusable (freed) bytes "+(totalFreed * dataSize));
    }

    @Override
    public long getAllocatedSize()
    {
        return this.totalAllocated;
    }

    @Override
    public long getUsedSize()
    {
        return ((current - totalFreed - 2) * dataSize);
    }

    private long computeAddress(long dataOffset, long fieldOffset)
    {
        assert dataOffset * dataSize + fieldOffset + 1 <= totalAllocated;
        return dataOffset * dataSize + baseAddress + fieldOffset;
    }

    @Override
    public int getInt(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getInt(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public boolean getBoolean(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getByte(computeAddress(dataOffset, fieldOffset)) == 1;
    }

    @Override
    public short getShort(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getShort(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public char getChar(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getChar(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public byte getByte(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getByte(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public long getLong(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getLong(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public float getFloat(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getFloat(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public double getDouble(int dataOffset, int fieldOffset)
    {
        return UNSAFE.getDouble(computeAddress(dataOffset, fieldOffset));
    }

    @Override
    public void setBoolean(int dataOffset, int fieldOffset, boolean value)
    {
        UNSAFE.putByte(computeAddress(dataOffset, fieldOffset), value ? (byte) 1 : 0);
    }

    @Override
    public void setInt(int dataOffset, int fieldOffset, int value)
    {
        UNSAFE.putInt(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setShort(int dataOffset, int fieldOffset, short value)
    {
        UNSAFE.putShort(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setChar(int dataOffset, int fieldOffset, char value)
    {
        UNSAFE.putChar(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setByte(int dataOffset, int fieldOffset, byte value)
    {
        UNSAFE.putByte(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setLong(int dataOffset, int fieldOffset, long value)
    {
        UNSAFE.putLong(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setFloat(int dataOffset, int fieldOffset, float value)
    {
        UNSAFE.putFloat(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public void setDouble(int dataOffset, int fieldOffset, double value)
    {
        UNSAFE.putDouble(computeAddress(dataOffset, fieldOffset), value);
    }

    @Override
    public Object getDataAsObject(int dataOffset)
    {
        Object o = dataArray[dataOffset];
        if (o instanceof WeakReferenceWithAddress)
        {
            o = ((WeakReferenceWithAddress)o).get();
        }
        return o;
    }

    @Override
    public MithraOffHeapDataObject getData(int dataOffset)
    {
        return (MithraOffHeapDataObject) this.getDataAsObject(dataOffset);
    }

    public void serializeSyncResult(ObjectOutput out, long maxClientReplicatedPageVersion) throws IOException
    {
        //body:
        FastUnsafeOffHeapPageBuffer pageBuffer = null;
        long maxPageVersion = 0;
        this.readWriteLock.acquireReadLock();
        FastUnsafeOffHeapIntList pagesToSend = maxClientReplicatedPageVersion > 0 ? new FastUnsafeOffHeapIntList(100) : new FastUnsafeOffHeapIntList(pageVersionList.size());
        try
        {
            try
            {
                maxPageVersion = scanPagesToSend(maxClientReplicatedPageVersion, pagesToSend);
                if (pagesToSend.size() > 0 && pagesToSend.size() <= MAX_PAGES_TO_COPY_UNDER_LOCK)
                {
                    pageBuffer = new FastUnsafeOffHeapPageBuffer(getPageSize(), pagesToSend.size(), pagesToSend);
                    for(int i=0;i<pagesToSend.size();i++)
                    {
                        int pageToSend = pagesToSend.get(i);
                        copyPageToBuffer(pageBuffer, i, pageToSend);
                    }
                }
            }
            finally
            {
                this.readWriteLock.release();
            }
            serializeSyncResultHeader(out, pagesToSend.size());
            if (pageBuffer != null)
            {
                pageBuffer.sendPages(out);
                out.writeInt(0);
            }
            else
            {
                // optimistically start sending
                sendPagesInBatches(maxClientReplicatedPageVersion, maxPageVersion, pagesToSend, out);
            }
        }
        finally
        {
            if (pagesToSend != null)
            {
                pagesToSend.destroy();
            }
            if (pageBuffer != null)
            {
                pageBuffer.destroy();
            }
        }
    }

    // Note: caller must ensure pagesToSend is empty before invoking this method
    // Method is protected for testing purposes only
    protected long scanPagesToSend(long maxClientReplicatedPageVersion, FastUnsafeOffHeapIntList pagesToSend)
    {
        long maxPageVersion = 0;
        boolean upgraded = false;
        for(int i=0;i<this.pageVersionList.size();i++)
        {
            if (pageVersionList.get(i) == 0 && !upgraded)
            {
                boolean originalLockWasReleased = this.readWriteLock.upgradeToWriteLock();
                this.currentPageVersion++;
                if (originalLockWasReleased)
                {
                    pagesToSend.clear();
                    for(int j=0;j<i;j++)
                    {
                        maxPageVersion = markAndAddPageIfRequired(maxClientReplicatedPageVersion, pagesToSend, maxPageVersion, j);
                    }
                }
                upgraded = true;
            }
            maxPageVersion = markAndAddPageIfRequired(maxClientReplicatedPageVersion, pagesToSend, maxPageVersion, i);
        }
        return maxPageVersion;
    }

    private long markAndAddPageIfRequired(long maxClientReplicatedPageVersion, FastUnsafeOffHeapIntList pagesToSend, long maxPageVersion, int pageNum)
    {
        if (pageVersionList.get(pageNum) == 0)
        {
            pageVersionList.set(pageNum, this.currentPageVersion);
        }
        long pageVersion = pageVersionList.get(pageNum);
        if (pageVersion > maxClientReplicatedPageVersion)
        {
            pagesToSend.add(pageNum);
        }
        if (pageVersion > maxPageVersion)
        {
            maxPageVersion = pageVersion;
        }
        return maxPageVersion;
    }

    private void sendPagesInBatches(long maxClientReplicatedPageVersion, long maxPageVersion, FastUnsafeOffHeapIntList pagesToSend, ObjectOutput out)
            throws IOException
    {
        FastUnsafeOffHeapPageBuffer pageBuffer = new FastUnsafeOffHeapPageBuffer(getPageSize(), MAX_PAGES_TO_COPY_UNDER_LOCK, new FastUnsafeOffHeapIntList(MAX_PAGES_TO_COPY_UNDER_LOCK));
        int offset = 0;
        try
        {
            while(offset < pagesToSend.size())
            {
                boolean restart = false;
                this.readWriteLock.acquireReadLock();
                try
                {
                    int end = MAX_PAGES_TO_COPY_UNDER_LOCK;
                    if (offset + end >= pagesToSend.size())
                    {
                        end = pagesToSend.size() - offset;
                    }
                    // ensure version numbers are still ok
                    for(int i=0;i<end;i++)
                    {
                        int pageToSend = pagesToSend.get(i+offset);
                        long pageVersion = this.pageVersionList.get(pageToSend);
                        if (pageVersion == 0 || pageVersion > maxPageVersion)
                        {
                            IntHashSet setOfPagesToSend = new IntHashSet(pagesToSend.size());
                            for(int j=offset;j<pagesToSend.size();j++)
                            {
                                setOfPagesToSend.add(pagesToSend.get(j));
                            }
                            pagesToSend.clear();
                            maxPageVersion = scanPagesToSend(maxPageVersion, pagesToSend);
                            for(int j=0;j<pagesToSend.size();j++)
                            {
                                setOfPagesToSend.add(pagesToSend.get(j));
                            }
                            pagesToSend.clear();
                            pagesToSend.addAll(setOfPagesToSend);
                            pagesToSend.sort();
                            offset = 0;
                            restart = true;
//                            System.out.println("restarting "+setOfPagesToSend);
                            break;
                        }
                    }
                    if (!restart)
                    {
//                        System.out.print("Copying pages ");
                        for(int i=0;i<end;i++)
                        {
                            int pageToSend = pagesToSend.get(i+offset);
//                            System.out.print(pageToSend+", ");
                            long pageVersion = this.pageVersionList.get(pageToSend);
                            copyPageToBuffer(pageBuffer, i, pageToSend);
                        }
                        offset += MAX_PAGES_TO_COPY_UNDER_LOCK;
//                        System.out.println();
                    }
                }
                finally
                {
                    this.readWriteLock.release();
                }
                if (!restart)
                {
                    pageBuffer.sendPages(out);
                    pageBuffer.clear();
                }
            }
        }
        finally
        {
            pageBuffer.destroy();
        }
        out.writeInt(0);
    }

    private void copyPageToBuffer(FastUnsafeOffHeapPageBuffer pageBuffer, int bufferPageIndex, int pageToSend)
    {
        int firstData = pageToSend << PAGE_POWER_OF_TWO;
        assert computeAddress(firstData, 0) - this.baseAddress + getPageSize() <= totalAllocated;

        pageBuffer.copyPage(pageToSend, this.pageVersionList.get(pageToSend), bufferPageIndex, computeAddress(firstData, 0));
        FastUnSafeOffHeapBitSet usedData = pageBuffer.getUsedData();
        usedData.clearPage(bufferPageIndex);
        for(int i = firstData;i < firstData + (1 << PAGE_POWER_OF_TWO); i++)
        {
            Object data = dataArray[i];
            if (data != null && !(data instanceof WeakReferenceWithAddress))
            {
                usedData.set(bufferPageIndex, i - firstData);
            }
        }
    }

    private void serializeSyncResultHeader(ObjectOutput out, int pageCountEstimate) throws IOException
    {
        //header:
        out.writeLong(this.finder.getMithraObjectPortal().getLatestRefreshTime());
        out.writeLong(getPageSize());
        out.writeInt(pageCountEstimate);
    }

    private long getPageSize()
    {
        return dataSize << PAGE_POWER_OF_TWO;
    }

    private static class WeakReferenceWithAddress extends WeakReference
    {
        private int address;

        private WeakReferenceWithAddress(Object referent, ReferenceQueue q, int address)
        {
            super(referent, q);
            this.address = address;
        }
    }

    @Override
    public void evictCollectedReferences()
    {
        if (!destroyed)
        {
            Object r;
            ReadWriteLock lock = null;
            try
            {
                while ((r = weakRefQueue.poll()) != null)
                {
                    if (lock == null)
                    {
                        lock = readWriteLock;
                        lock.acquireWriteLock();
                    }
                    WeakReferenceWithAddress refWithAddress = (WeakReferenceWithAddress) r;
                    if (dataArray[refWithAddress.address] == refWithAddress)
                    {
                        dataArray[refWithAddress.address] = null;
                        while(true)
                        {
                            long curStackHead = freeStackHeadAndSize;
                            int stackHead = getStackHead(curStackHead);
                            setInt(refWithAddress.address, 0, stackHead);
                            if (casFreeStackHead(curStackHead, refWithAddress.address, 1))
                            {
                                break;
                            }
                        }
                    }
                }
            }
            finally
            {
                if (lock != null) lock.release();
            }
        }
    }

    // for test purposes only
    private FastUnsafeOffHeapLongList zGetPageVersionList()
    {
        return pageVersionList;
    }

    private class SyncLog
    {
        private String className;
        private long startTime = System.currentTimeMillis();
        private long masterResultReceivedTime;
        private long stringFixUpTime;
        private long endTime;
        private int buffers;
        private int pages;
        private boolean initialSync;
        private boolean updateViaCopy;
        private boolean processInPlace;
        private int toInsertInExistingPages;
        private int toUpdateExisting;
        private int toRemoveExisting;
        private int toNukeAndInsert;
        private long initialMaxPageVersion;
        private long finalMaxPageVersion;

        private SyncLog(String className)
        {
            this.className = className;
        }

        public void logMasterResultReceived(MasterSyncResult result)
        {
            masterResultReceivedTime = System.currentTimeMillis();
            this.buffers = result.getBuffers().size();
            this.pages = result.getPageLocationMap().size();
        }

        public void logStringsFixedUp()
        {
            stringFixUpTime = System.currentTimeMillis();
        }

        public void logSyncFinished()
        {
            endTime = System.currentTimeMillis();
        }

        public void setInitialSync()
        {
            this.initialSync = true;
        }

        public void setUpdateViaCopy()
        {
            this.updateViaCopy = true;
        }

        public void setToInsertInExistingPages(int toInsertInExistingPages)
        {
            this.toInsertInExistingPages = toInsertInExistingPages;
        }

        public void setToUpdateExisting(int toUpdateExisting)
        {
            this.toUpdateExisting = toUpdateExisting;
        }

        public void setToRemoveExisting(int toRemoveExisting)
        {
            this.toRemoveExisting = toRemoveExisting;
        }

        public void setToNukeAndInsert(int toNukeAndInsert)
        {
            this.toNukeAndInsert = toNukeAndInsert;
        }

        public void setProcessInPlace()
        {
            this.processInPlace = true;
        }

        public void setInitialMaxPageVersion(long initialMaxPageVersion)
        {
            this.initialMaxPageVersion = initialMaxPageVersion;
        }

        public void setFinalMaxPageVersion(long finalMaxPageVersion)
        {
            this.finalMaxPageVersion = finalMaxPageVersion;
        }

        public void printLog()
        {
            if (logger.isDebugEnabled())
            {
                StringBuffer buffer = new StringBuffer(400);
                buffer.append("Cache replication sync for ").append(businessClassName).append("\n");
                buffer.append("network time ").append((masterResultReceivedTime - startTime)/1000.0).append(" s");
                if (buffers == 0)
                {
                    buffer.append(". done.");
                }
                else
                {
                    buffer.append("\nstring fixup time ").append((stringFixUpTime - masterResultReceivedTime)/1000.0).append(" s\n");
                    buffer.append("processing time ").append((endTime - stringFixUpTime)/1000.0).append(" s\n");
                    buffer.append("total time ").append((endTime - startTime)/1000.0).append(" s\n");
                    buffer.append(pages).append(" incoming pages in ").append(buffers).append(" buffers\n");
                    int pageCount = (current >> PAGE_POWER_OF_TWO) + 1;
                    if (initialSync)
                    {
                        buffer.append("initial sync of ").append(pageCount).append(" data pages");
                    }
                    else
                    {
                        if (processInPlace)
                        {
                            buffer.append("in place processed records\n");
                        }
                        else
                        {
                            buffer.append("processed via copy\n");
                            buffer.append("final page count ").append(pageCount).append("\n");
                        }
                        appendIfNonZero(buffer, toInsertInExistingPages, " inserted in existing pages\n");
                        appendIfNonZero(buffer, toRemoveExisting, " removed\n");
                        appendIfNonZero(buffer, toUpdateExisting, " updated\n");
                        appendIfNonZero(buffer, toNukeAndInsert, " replaced with different\n");
                    }
                }
                logger.debug(buffer.toString());
            }
        }

        private void appendIfNonZero(StringBuffer buffer, int num, String msg)
        {
            if (num > 0)
            {
                buffer.append(num).append(msg);
            }
        }
    }
}

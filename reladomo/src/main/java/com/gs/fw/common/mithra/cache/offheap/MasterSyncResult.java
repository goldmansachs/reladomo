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


import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class MasterSyncResult implements Externalizable
{
    private transient FastUnsafeOffHeapDataStorage masterStorage;
    private transient long maxClientReplicatedPageVersion;
    private FastList<FastUnsafeOffHeapPageBuffer> buffers;
    private IntLongHashMap pageLocationMap;
    private long maxReplicatedVersion;
    private long lastMasterRefreshTime;

    public MasterSyncResult()
    {
        // for externalizable
    }

    public MasterSyncResult(FastUnsafeOffHeapDataStorage masterStorage, long maxClientReplicatedPageVersion)
    {
        this.masterStorage = masterStorage;
        this.maxClientReplicatedPageVersion = maxClientReplicatedPageVersion;
    }

    public FastList<FastUnsafeOffHeapPageBuffer> getBuffers()
    {
        return buffers;
    }

    public IntLongHashMap getPageLocationMap()
    {
        return pageLocationMap;
    }

    public long getMaxReplicatedVersion()
    {
        return maxReplicatedVersion;
    }

    public long getLastMasterRefreshTime()
    {
        return lastMasterRefreshTime;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.lastMasterRefreshTime = in.readLong();
        long pageSize = in.readLong();
        int totalPageEstimate = in.readInt();
        int pagesToFollow = in.readInt();
        this.pageLocationMap = new IntLongHashMap(totalPageEstimate);
        this.buffers = FastList.newList(pagesToFollow);
        if (pagesToFollow == 0)
        {
            return;
        }
        FastUnsafeOffHeapPageBuffer lastBuffer = new FastUnsafeOffHeapPageBuffer(pageSize, totalPageEstimate);
        buffers.add(lastBuffer);
        while(pagesToFollow > 0)
        {
            for(int i=0;i<pagesToFollow;i++)
            {
                int masterPageIndex = in.readInt();
                long combinedPageLocation = pageLocationMap.get(masterPageIndex);
                if (combinedPageLocation == 0)
                {
                    if (!lastBuffer.hasMoreRoom())
                    {
                        lastBuffer = new FastUnsafeOffHeapPageBuffer(pageSize, 10);
                        buffers.add(lastBuffer);
                    }
                    int bufferPageIndex = lastBuffer.readNewPage(in, masterPageIndex);
                    long pageVersion = lastBuffer.getMasterPageVersions().get(bufferPageIndex);
                    if (pageVersion > maxReplicatedVersion)
                    {
                        this.maxReplicatedVersion = pageVersion;
                    }
                    pageLocationMap.put(masterPageIndex, (((long) buffers.size()) << 32) | bufferPageIndex);
                }
                else
                {
                    int pageBufferLocation = getPageBufferLocation(combinedPageLocation);
                    int bufferPageIndex = getBufferPageIndex(combinedPageLocation);
                    FastUnsafeOffHeapPageBuffer buffer = buffers.get(pageBufferLocation);
                    buffer.readExistingPage(in, bufferPageIndex);
                    long pageVersion = buffer.getMasterPageVersions().get(bufferPageIndex);
                    if (pageVersion > maxReplicatedVersion)
                    {
                        this.maxReplicatedVersion = pageVersion;
                    }
                }
            }
            pagesToFollow = in.readInt();
        }
    }

    public int getBufferPageIndex(long combinedPageLocation)
    {
        return (int) (combinedPageLocation & 0xFFFFFFFF);
    }

    public int getPageBufferLocation(long combinedPageLocation)
    {
        return ((int)(combinedPageLocation >> 32)) - 1;
    }

    /*
    The protocol:
        header:
            long: lastRefreshTime
            long: pageSize
            int: estimate of the number of pages being sent. More pages may be sent than the estimate, but not less.
        body:
            int: number of pages to follow. If 0, we're at end of stream
            for each page:
                int: page index
                long: page version
                bytes: page content
                bytes representing a bit set: used data
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        masterStorage.serializeSyncResult(out, maxClientReplicatedPageVersion);
    }

    public void destroy()
    {
        if (this.buffers != null)
        {
            for(int i=0;i<this.buffers.size();i++)
            {
                this.buffers.get(i).destroy();
            }
        }
    }

    public void fixUpStringReferences(OffHeapStringExtractor[] stringAttributes, MasterCacheUplink uplink, long dataSize)
    {
        for(int i=0;i<this.buffers.size();i++)
        {
            this.buffers.get(i).fixUpStringReferences(stringAttributes, uplink, dataSize);
        }
    }

    public boolean isEmpty()
    {
        return this.buffers.isEmpty();
    }

}

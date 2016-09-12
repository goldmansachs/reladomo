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


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FastUnsafeOffHeapPageBuffer extends OffHeapMemoryReference
{
    private long pageSize;
    private int allocatedPages;
    private FastUnsafeOffHeapIntList masterPageIndicies;
    private FastUnsafeOffHeapLongList masterPageVersions;
    private FastUnSafeOffHeapBitSet usedData;

    public FastUnsafeOffHeapPageBuffer(long pageSize, int pagesToAllocate, FastUnsafeOffHeapIntList masterPageIndicies)
    {
        super(pageSize* pagesToAllocate);
        this.pageSize = pageSize;
        this.allocatedPages = pagesToAllocate;
        this.masterPageIndicies = masterPageIndicies;
        this.masterPageVersions = new FastUnsafeOffHeapLongList(pagesToAllocate);
        this.registerForGarbageCollection();
        this.usedData = new FastUnSafeOffHeapBitSet(this.allocatedPages);
    }

    public FastUnsafeOffHeapPageBuffer(long pageSize, int pagesToAllocate)
    {
        this(pageSize, pagesToAllocate, new FastUnsafeOffHeapIntList(pagesToAllocate));
    }

    public void copyPage(int pageToSend, long pageVersion, int bufferPageIndex, long pageAddress)
    {
        this.masterPageIndicies.set(bufferPageIndex, pageToSend);
        this.masterPageVersions.set(bufferPageIndex, pageVersion);
        UNSAFE.copyMemory(pageAddress, bufferPageIndex * pageSize + this.getBaseAddress(), pageSize);
        if (allocatedPages <= bufferPageIndex)
        {
            allocatedPages = bufferPageIndex + 1;
        }
    }

    public int getAllocatedPages()
    {
        return allocatedPages;
    }

    public int getUsedPages()
    {
        return this.masterPageIndicies.size();
    }

    public FastUnSafeOffHeapBitSet getUsedData()
    {
        return usedData;
    }

    public FastUnsafeOffHeapIntList getMasterPageIndicies()
    {
        return masterPageIndicies;
    }

    public FastUnsafeOffHeapLongList getMasterPageVersions()
    {
        return masterPageVersions;
    }

    @Override
    public synchronized void destroy()
    {
        super.destroy();
        this.masterPageIndicies.destroy();
        this.masterPageVersions.destroy();
        this.usedData.destroy();
    }

    public void sendPages(ObjectOutput out) throws IOException
    {
        out.writeInt(allocatedPages);
        for(int i=0;i< allocatedPages;i++)
        {
            out.writeInt(masterPageIndicies.get(i));
            out.writeLong(masterPageVersions.get(i));
            for(long j=0;j<pageSize;j++)
            {
                out.writeByte(UNSAFE.getByte(this.getBaseAddress() + i*pageSize + j));
            }
            usedData.serializePage(out, i);
        }
    }

    public void clear()
    {
        this.allocatedPages = 0;
    }

    public boolean hasMoreRoom()
    {
        return this.masterPageIndicies.size() < allocatedPages;
    }

    public int readNewPage(ObjectInput in, int masterPageIndex) throws IOException
    {
        long pageVersion = in.readLong();
        this.masterPageIndicies.add(masterPageIndex);
        this.masterPageVersions.add(pageVersion);
        int bufferPageIndex = this.masterPageIndicies.size() - 1;
        readPageAndUsedData(in, bufferPageIndex);
        return bufferPageIndex;
    }

    private void readPageAndUsedData(ObjectInput in, int bufferPageIndex) throws IOException
    {
        long pageStart = getPageStartLocation(bufferPageIndex);
        long pageEnd = pageStart + pageSize;
        assert pageStart >= this.getBaseAddress();
        assert pageEnd <= this.getBaseAddress() + this.getAllocatedLength();
        for(long j=pageStart;j<pageEnd;j++)
        {
            UNSAFE.putByte(j, in.readByte());
        }
        usedData.deserializePage(in, bufferPageIndex);
    }

    public void readExistingPage(ObjectInput in, int bufferPageIndex) throws IOException
    {
        long pageVersion = in.readLong();
        this.masterPageVersions.set(bufferPageIndex, pageVersion);
        readPageAndUsedData(in, bufferPageIndex);
    }

    public void fixUpStringReferences(OffHeapStringExtractor[] stringAttributes, MasterCacheUplink uplink, long dataSize)
    {
        for(int i=0;i< allocatedPages;i++)
        {
            for(long j=0;j<pageSize;j+=dataSize)
            {
                for(OffHeapStringExtractor extractor: stringAttributes)
                {
                    extractor.convertMasterStringToLocalString(this.getBaseAddress() + j + i * pageSize, uplink);
                }
            }
        }
    }

    public long getPageStartLocation(int bufferPageIndex)
    {
        return this.getBaseAddress() + bufferPageIndex * pageSize;
    }

    public void destroyWithoutBufferDeallocation()
    {
        this.setDestroyed();
        this.masterPageIndicies.destroy();
        this.masterPageVersions.destroy();
        this.usedData.destroy();
    }
}

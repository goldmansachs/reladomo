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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraManagerProvider;


public class MithraCpuBoundThreadPool
{
    public static final int WEIGHT_ONE_CHUNK_SIZE = 1 << 15;
    private static int parallelThreshold = WEIGHT_ONE_CHUNK_SIZE << 2;
    private static final String MITHRA_CPU_THREADS = "mithra.cpu.threads";
    private static Logger logger = LoggerFactory.getLogger(MithraCpuBoundThreadPool.class.getName());

    private static volatile MithraCpuBoundThreadPool instance;
    private static final ThreadGroup threadGroup = new ThreadGroup("Mithra CPU Bound Thread Group");

    private PeekableQueue peekableQueue = new PeekableQueue();
    private int threads;
    private final AtomicInteger threadNumber = new AtomicInteger(0);

    public static MithraCpuBoundThreadPool getInstance()
    {
        if (instance == null)
        {
            synchronized (MithraCpuBoundThreadPool.class)
            {
                if (instance == null)
                {
                    instance = new MithraCpuBoundThreadPool();
                }
            }
        }
        return instance;
    }

    public MithraCpuBoundThreadPool()
    {
        try
        {
            this.threads = Integer.parseInt(System.getProperty(MITHRA_CPU_THREADS, ""+Runtime.getRuntime().availableProcessors()));
        }
        catch (NumberFormatException e)
        {
            logger.warn("Could not parse "+MITHRA_CPU_THREADS+" system property with value "+System.getProperty(MITHRA_CPU_THREADS), e);
            this.threads = Runtime.getRuntime().availableProcessors();
        }
        for(int i=0;i<this.threads;i++)
        {
            new MithraCpuBoundThread().start();
        }
    }

    public int getThreads()
    {
        return threads;
    }

    public static boolean isParallelizable(int count)
    {
        return count >= parallelThreshold && !MithraManagerProvider.getMithraManager().isInTransaction();
    }

    public int getParallelThreshold()
    {
        return parallelThreshold;
    }

    public static void setParallelThreshold(int threshold)
    {
        parallelThreshold = threshold;
        if (parallelThreshold < 2) parallelThreshold = 2;
    }

    public int computeChunkSize(int count, int chunks)
    {
        int chunkSize = count/chunks;
        if (count % chunks > 0)
        {
            chunkSize++;
        }
        return chunkSize;
    }

    public int computeChunks(int count, int chunkSize)
    {
        int chunks = count/chunkSize;
        if (count % chunkSize > 0)
        {
            chunks++;
        }
        return chunks;
    }

    public List<List> split(List dataHolders)
    {
        List<List> lists;
        if (dataHolders instanceof MithraCompositeList)
        {
            lists = ((MithraCompositeList)dataHolders).getLists();
        }
        else
        {
            int chunks = this.getThreads();
            int chunkSize = this.getParallelThreshold();
            int dataHoldersSize = dataHolders.size();
            if (dataHoldersSize /chunks >= chunkSize)
            {
                chunkSize = this.computeChunkSize(dataHoldersSize, chunks);
                chunks = this.computeChunks(dataHoldersSize, chunkSize);
            }
            else
            {
                chunks = this.computeChunks(dataHoldersSize, chunkSize);
                chunkSize = this.computeChunkSize(dataHoldersSize, chunks);
            }
            lists = new FastList<List>(chunks);
            for(int i=0;i<chunks;i++)
            {
                int start = i*chunkSize;
                int end = Math.min((i+1)*chunkSize, dataHoldersSize);
                lists.add(dataHolders.subList(start, end));
            }
        }
        return lists;
    }

    public void submitTaskFactory(TaskFactory taskFactory)
    {
        this.peekableQueue.add(taskFactory);
    }

    public void setFactoryDone(TaskFactory taskFactory)
    {
        this.peekableQueue.removeIfSame(taskFactory);
    }

    private class MithraCpuBoundThread extends Thread
    {
        private MithraCpuBoundThread()
        {
            super(threadGroup, "Mithra CPU Bound-" + threadNumber.incrementAndGet());
            this.setDaemon(true);
        }

        @Override
        public void run()
        {

            while(true)
            {
                try
                {
                    TaskFactory factory = (TaskFactory) peekableQueue.peekBlockIfEmpty();
                    Runnable task = factory.createTask();
                    if (task == null)
                    {
                        peekableQueue.removeIfSame(factory);
                    }
                    else
                    {
                        task.run();
                    }
                }
                catch (Throwable e)
                {
                    logger.error("Uncaught exception in runnable", e);
                }
            }
        }
    }
}

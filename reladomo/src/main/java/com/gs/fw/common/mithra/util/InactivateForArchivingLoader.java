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


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InactivateForArchivingLoader
{
    private static final Object END = new Object();

    private static Logger logger = LoggerFactory.getLogger(InactivateForArchivingLoader.class.getName());

    private RelatedFinder finder;
    private AsOfAttribute businessDate;
    private AsOfAttribute processingDate;
    private Timestamp startTime;
    private Timestamp endTime;
    private int batchSize = 10000;
    private LinkedBlockingQueue queue = new LinkedBlockingQueue(50);
    private Attribute[] indexExtractor;
    private Object sourceSourceAttribute;
    private Object destinationSourceAttribute;
    private int updateThreads = 1;
    private int destinationReaderThreads = 5;
    private long lastLogTime;
    private int sourceRowsRead;
    private AtomicInteger destinationRowsRead = new AtomicInteger();
    private Throwable error;
    private static final long LOG_PERIOD = 60000;

    public InactivateForArchivingLoader(Timestamp startTime, Timestamp endTime, RelatedFinder finder, Object sourceSourceAttribute, Object destinationSourceAttribute)
    {
        this.startTime = startTime;
        this.endTime = endTime;
        this.finder = finder;
        this.sourceSourceAttribute = sourceSourceAttribute;
        this.destinationSourceAttribute = destinationSourceAttribute;
        this.lastLogTime = System.currentTimeMillis();

        AsOfAttribute[] finderAsOfAttributes = finder.getAsOfAttributes();
        if (finderAsOfAttributes.length == 2)
        {
            businessDate = finderAsOfAttributes[0];
            processingDate = finderAsOfAttributes[1];
        }
        if (finderAsOfAttributes.length == 1)
        {
            if (finderAsOfAttributes[0].isProcessingDate())
            {
                processingDate = finderAsOfAttributes[0];
            }
            else
            {
                throw new RuntimeException("Chained inactivation is only supported with processing date");
            }
        }

        Attribute[] primaryKeyAttributes = finder.getPrimaryKeyAttributes();
        indexExtractor = new Attribute[primaryKeyAttributes.length  + finderAsOfAttributes.length - 1];
        System.arraycopy(primaryKeyAttributes, 0, indexExtractor, 0, primaryKeyAttributes.length - 1); // don't copy the source attribute
        for(int i=0;i<finderAsOfAttributes.length;i++)
        {
            indexExtractor[primaryKeyAttributes.length + i - 1] = finderAsOfAttributes[i].getFromAttribute();
        }
    }

    private void logProgress(boolean force)
    {
        long now = System.currentTimeMillis();
        if (force || this.lastLogTime < now - LOG_PERIOD)
        {
            this.lastLogTime = now;
            logger.info("Source rows read: "+sourceRowsRead+" Destination rows read: "+destinationRowsRead);
        }
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public void setUpdateThreads(int updateThreads)
    {
        this.updateThreads = updateThreads;
    }

    public void setDestinationReaderThreads(int destinationReaderThreads)
    {
        this.destinationReaderThreads = destinationReaderThreads;
    }

    public void startAndWaitUntilFinished()
    {
        Operation destinationOp = processingDate.eq(processingDate.getInfinityDate());
        destinationOp = destinationOp.and(finder.getSourceAttribute().nonPrimitiveEq(destinationSourceAttribute));
        if (businessDate != null)
        {
            destinationOp = destinationOp.and(businessDate.equalsEdgePoint());
        }
        TupleAttribute tupleAttribute = indexExtractor[0].tupleWith(indexExtractor[1]);
        for(int i=2; i < indexExtractor.length; i++)
        {
            tupleAttribute = tupleAttribute.tupleWith(indexExtractor[i]);
        }
        final InactivateForArchiveSingleQueueExecutor sqe = new InactivateForArchiveSingleQueueExecutor(updateThreads, indexExtractor[0].ascendingOrderBy(), batchSize, finder, 0);
        Thread sourceReader = new Thread(new SourceReader());
        sourceReader.start();
        Thread[] destinationReaders = new Thread[destinationReaderThreads];
        for(int i=0;i<destinationReaderThreads;i++)
        {
            destinationReaders[i] = new Thread(new DestinationReader(destinationOp, sqe, tupleAttribute));
            destinationReaders[i].start();
        }
        joinWithoutException(sourceReader);
        for(int i=0;i<destinationReaderThreads;i++)
        {
            joinWithoutException(destinationReaders[i]);
        }
        logProgress(true);
        sqe.waitUntilFinished();
        checkExceptions();
    }

    private void checkExceptions()
    {
        if (this.error != null)
        {
            throw new RuntimeException("loader did not complete.", error);
        }
    }

    private void joinWithoutException(Thread sourceReader)
    {
        while(true)
        {
            try
            {
                sourceReader.join();
                break;
            }
            catch (InterruptedException e)
            {
                //ignore and try again
            }
        }
    }

    private class SourceReader implements Runnable
    {
        public void run()
        {
            try
            {
                Operation fromOp = processingDate.getFromAttribute().lessThanEquals(startTime); // objects inserted after startTime will be handled later via insertForRecovery
                Operation toOp = processingDate.getToAttribute().lessThanEquals(endTime);
                toOp = toOp.and(processingDate.getToAttribute().greaterThan(startTime));

                Operation op = processingDate.equalsEdgePoint().and(fromOp.and(toOp));
                op = op.and(finder.getSourceAttribute().nonPrimitiveEq(sourceSourceAttribute));
                if (businessDate != null)
                {
                    op = op.and(businessDate.equalsEdgePoint());
                }
                MithraList list = finder.findMany(op);
                final FastList[] buffers = new FastList[1];
                buffers[0] = new FastList(batchSize);
                list.forEachWithCursor(new DoWhileProcedure()
                {
                    public boolean execute(Object each)
                    {
                        buffers[0].add(each);
                        sourceRowsRead++;
                        if (buffers[0].size() == batchSize)
                        {
                            putWithoutException(queue, buffers);
                            buffers[0] = new FastList(batchSize);
                        }
                        logProgress(false);
                        return true;
                    }
                });
                if (buffers[0].size() > 0)
                {
                    putWithoutException(queue, buffers[0]);
                }
            }
            catch (Throwable e)
            {
                logger.error("Error while reading source", e);
                error = e;
            }
            finally
            {
                for(int i=0;i<destinationReaderThreads;i++)
                {
                    putWithoutException(queue, END);
                }
            }
            logProgress(true);
        }
    }

    private void putWithoutException(LinkedBlockingQueue queue, Object o)
    {
        while(true)
        {
            try
            {
                queue.put(o);
                break;
            }
            catch (InterruptedException e)
            {
                //ignore - try again
            }
        }
    }

    private class DestinationReader implements Runnable
    {
        private TupleAttribute tupleAttribute;
        private Operation destinationOp;
        private SingleQueueExecutor sqe;

        private DestinationReader(Operation destinationOp, SingleQueueExecutor sqe, TupleAttribute tupleAttribute)
        {
            this.destinationOp = destinationOp;
            this.sqe = sqe;
            this.tupleAttribute = tupleAttribute;
        }

        public void run()
        {
            final FullUniqueIndex index = new FullUniqueIndex(indexExtractor, batchSize);
            try
            {
                while(true)
                {
                    Object o;
                    try
                    {
                        o = queue.take();
                    }
                    catch (InterruptedException e)
                    {
                        continue; // ignore and try again
                    }
                    if (o == END)
                    {
                        break;
                    }
                    FastList list = (FastList) o;
                    index.clear();
                    for(int i=0;i<list.size();i++)
                    {
                        index.put(list.get(i));
                    }
                    Operation op = destinationOp.and(tupleAttribute.in(list, indexExtractor));
                    MithraList dbList = finder.findMany(op);
                    dbList.forEachWithCursor(new DoWhileProcedure()
                    {
                        public boolean execute(Object destinationObject)
                        {
                            destinationRowsRead.incrementAndGet();
                            Object sourceObject = index.getFromData(destinationObject);
                            if (sourceObject != null)
                            {
                                sqe.addForUpdate(destinationObject, sourceObject);
                            }
                            else
                            {
                                logger.warn("Could not find destination object for "+((MithraDatedTransactionalObject)sourceObject).zGetCurrentData().zGetPrintablePrimaryKey());
                            }
                            logProgress(false);
                            return true;
                        }
                    });
                    logProgress(true);
                }
            }
            catch (Exception e)
            {
                logger.error("Unexpected exception in destination reader", e);
                error = e;
            }
        }
    }
}

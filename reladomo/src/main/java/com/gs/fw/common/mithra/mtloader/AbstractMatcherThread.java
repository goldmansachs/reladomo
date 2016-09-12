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

package com.gs.fw.common.mithra.mtloader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.QueueExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractMatcherThread<T> extends Thread
{
    protected static Logger logger = LoggerFactory.getLogger(AbstractMatcherThread.class);

    protected static final Comparator DEFAULT_COMPARATOR = new AbstractMatcherThread.DefaultComparator();
    public static final int DEFAULT_MAX_AHEAD = 50000000;
    protected static final int DEFAULT_INDEX_SIZE = 1000000;

    protected final List<Callable> thingsToDo = new ArrayList<Callable>(10000);
    protected boolean fileDone;
    protected boolean dbDone;
    protected Comparator comparator = AbstractMatcherThread.DEFAULT_COMPARATOR;
    protected FullUniqueIndex<T> fileIndex;
    protected FullUniqueIndex<T> dbIndex;
    protected final Object finishedLock = new Object();
    protected boolean finished = false;
    protected int totalFile = 0;
    protected int totalDb = 0;
    protected final Object waitForOther = new Object();
    protected QueueExecutor executor;
    protected AbortException abortException;
    protected int maxAhead = DEFAULT_MAX_AHEAD;

    public AbstractMatcherThread(QueueExecutor queueExecutor, Extractor[] extractor)
    {
        this.setName("Matcher thread");
        this.executor = queueExecutor;

        fileIndex = new FullUniqueIndex(extractor, DEFAULT_INDEX_SIZE);
        dbIndex = new FullUniqueIndex(extractor, DEFAULT_INDEX_SIZE);
    }

    public AbstractMatcherThread(QueueExecutor queueExecutor, Extractor[] extractor, int indexSize)
    {
        this.setName("Matcher thread");
        this.executor = queueExecutor;

        if (indexSize <= 0)
        {
            indexSize = DEFAULT_INDEX_SIZE;
        }
        fileIndex = new FullUniqueIndex(extractor, indexSize);
        dbIndex = new FullUniqueIndex(extractor, indexSize);
    }

    public abstract Callable constructDbDoneCallable();

    public abstract Callable constructFileDoneCallable();

    protected abstract void processRecords(List<T> records, FullUniqueIndex<T> index, FullUniqueIndex<T> matchIndex, boolean fromFile);

    protected abstract void processFileRecordsForInsert(List<T> fileRecords);

    protected abstract void processDbRecordsForTermination(List<T> dbRecords);

    public void setComparator(Comparator comparator)
    {
        this.comparator = comparator;
    }

    public void setMustAbort(Throwable cause)
    {
        synchronized (thingsToDo)
        {
            this.abortException = new AbortException("unexpected abort", cause);
            thingsToDo.notify();
        }
    }

    protected void checkForAbort()
            throws AbortException
    {
        if (this.abortException != null)
        {
            throw this.abortException;
        }
    }

    public int getMaxAhead()
    {
        return maxAhead;
    }

    public void setMaxAhead(int maxAhead)
    {
        this.maxAhead = maxAhead;
    }

    public void setFileDone()
    {
        synchronized (thingsToDo)
        {
            if (this.abortException != null)
            {
                return;
            }
            thingsToDo.add(constructFileDoneCallable());
            thingsToDo.notify();
        }
    }

    public void setDbDone()
    {
        synchronized (thingsToDo)
        {
            if (this.abortException != null)
            {
                return;
            }
            thingsToDo.add(constructDbDoneCallable());
            thingsToDo.notify();
        }
    }

    public FullUniqueIndex getFileIndex()
    {
        return fileIndex;
    }

    public FullUniqueIndex getDbIndex()
    {
        return dbIndex;
    }

    public void addFileRecords(List<T> records) throws AbortException
    {
        AbstractMatcherThread.FileRecords fileRecords = new AbstractMatcherThread.FileRecords(records);
        synchronized (thingsToDo)
        {
            checkForAbort();
            thingsToDo.add(fileRecords);
            thingsToDo.notify();
        }
        synchronized (this.waitForOther)
        {
            if (!dbDone && totalFile > totalDb + maxAhead)
            {
                try
                {
                    this.waitForOther.wait();
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }
    }

    public void addDbRecords(List<T> records) throws AbortException
    {
        AbstractMatcherThread.DbRecords dbRecords = new AbstractMatcherThread.DbRecords(records);
        synchronized (thingsToDo)
        {
            checkForAbort();
            thingsToDo.add(dbRecords);
            thingsToDo.notify();
        }
    }

    public void waitTillDone() throws AbortException
    {
        synchronized (this.finishedLock)
        {
            while (!finished)
            {
                try
                {
                    checkForAbort();
                    this.finishedLock.wait();
                }
                catch (InterruptedException e)
                {
                    // huh?
                }
            }
        }
        checkForAbort();
    }

    public void run()
    {
        boolean done;
        synchronized (this.finishedLock)
        {
            done = fileDone && dbDone;
        }

        try
        {
            while (!done)
            {
                Callable toDo = null;
                synchronized (thingsToDo)
                {
                    if (abortException == null)
                    {
                        if (thingsToDo.isEmpty())
                        {
                            try
                            {
                                thingsToDo.wait();
                            }
                            catch (InterruptedException e)
                            {
                                // whatever
                            }
                        }
                        else
                        {
                            toDo = thingsToDo.remove(0);
                        }
                    }
                }
                if (toDo != null)
                {
                    toDo.call();
                }
                synchronized (this.finishedLock)
                {
                    done = fileDone && dbDone;
                }
                synchronized (thingsToDo)
                {
                    done = (this.abortException != null) || (done && thingsToDo.isEmpty());
                }
            }

            executor.flushUpdate();
            executor.waitUntilFinished();
        }
        catch (AbortException ex)
        {
            this.abortException = ex;
        }
        catch (Throwable ex)
        {
            this.abortException = new AbortException("Unexpected Exception.", ex);
        }

        synchronized (this.finishedLock)
        {
            this.finished = true;
            finishedLock.notifyAll();
        }
    }

    protected void processFileRecords(List<T> fileRecords)
    {
        processRecords(fileRecords, fileIndex, dbIndex, true);
        synchronized (this.waitForOther)
        {
            this.totalFile += fileRecords.size();
        }
    }

    protected void processDbRecords(List<T> dbRecords)
    {
        processRecords(dbRecords, dbIndex, fileIndex, false);
        synchronized (this.waitForOther)
        {
            this.totalDb += dbRecords.size();
            if (totalDb + maxAhead >= totalFile)
            {
                this.waitForOther.notify();
            }
        }
    }

    public class FileRecords implements Callable
    {
        private List<T> fileRecords;

        public FileRecords(List<T> fileRecords)
        {
            this.fileRecords = fileRecords;
        }

        public Object call() throws Exception
        {
            if (!dbDone)
            {
                processFileRecords(fileRecords);
            }
            else
            {
                processFileRecordsForInsert(fileRecords);
            }
            return null;
        }
    }

    public class DbRecords implements Callable
    {
        private List<T> dbRecords;

        public DbRecords(List<T> dbRecords)
        {
            this.dbRecords = dbRecords;
        }

        public Object call() throws Exception
        {
            if (!fileDone)
            {
                processDbRecords(dbRecords);
            }
            else
            {
                processDbRecordsForTermination(dbRecords);
            }
            return null;
        }
    }

    private static class DefaultComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            MithraTransactionalObject t1 = (MithraTransactionalObject) o1;
            MithraTransactionalObject t2 = (MithraTransactionalObject) o2;
            if (t1.nonPrimaryKeyAttributesChanged(t2))
            {
                return 1;
            }
            return 0;
        }
    }
}

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

import java.util.List;
import java.util.concurrent.Callable;

import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.QueueExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MatcherThread<T> extends AbstractMatcherThread<T>
{
    protected static Logger logger = LoggerFactory.getLogger(MatcherThread.class);

    public MatcherThread(QueueExecutor queueExecutor, Extractor[] extractor)
    {
        super(queueExecutor, extractor);
    }

    public MatcherThread(QueueExecutor queueExecutor, Extractor[] extractor, int indexSize)
    {
        super(queueExecutor, extractor, indexSize);
    }

    public Callable constructDbDoneCallable()
    {
        return new DatabaseDoneCallable();
    }

    public Callable constructFileDoneCallable()
    {
        return new FileDoneCallable();
    }

    private class DatabaseDoneCallable implements Callable
    {

        public Object call() throws Exception
        {
            executor.flushTermination();
            int count = 0;
            List<T> fileDataRecords = fileIndex.getAll();
            fileIndex.clear();
            while (count < fileDataRecords.size())
            {
                handleInsert(fileDataRecords.get(count++));
            }
            executor.flushInsert();
            synchronized (finishedLock)
            {
                dbDone = true;
            }
            synchronized (waitForOther)
            {
                waitForOther.notify();
            }
            handleMatcherFinishedWithDatabaseSide();
            return null;
        }
    }

    private class FileDoneCallable implements Callable
    {
        public Object call() throws Exception
        {
            executor.flushInsert();
            int count = 0;
            List<T> dbDataRecords = dbIndex.getAll();
            dbIndex.clear();
            while (count < dbDataRecords.size())
            {
                handleDeleteOrTerminate(dbDataRecords.get(count++));
            }
            executor.flushTermination();
            synchronized (finishedLock)
            {
                fileDone = true;
            }
            handleMatcherFinishedWithFileSide();
            return null;
        }
    }

    protected void processRecords(List<T> records, FullUniqueIndex<T> index, FullUniqueIndex<T> matchIndex, boolean fromFile)
    {
        for (int i = 0; i < records.size(); i++)
        {
            T o = records.get(i);
            T match = matchIndex.remove(o);
            if (match == null)
            {
                index.put(o);
            }
            else
            {
                if (fromFile)
                {
                    compareAndUpdateIfNecessary(match, o);
                }
                else
                {
                    compareAndUpdateIfNecessary(o, match);
                }
            }
        }
    }

    protected void compareAndUpdateIfNecessary(T oldObject, T newObject)
    {
        handleKeyMatchedBeforeCompare(oldObject, newObject);
        if (this.comparator.compare(oldObject, newObject) != 0)
        {
            handleUpdate(oldObject, newObject);
        }
        else
        {
            handleIdenticalMatch(oldObject, newObject);
        }
    }

    protected void processFileRecordsForInsert(List<T> fileRecords)
    {
        for (int i = 0; i < fileRecords.size(); i++)
        {
            T o = fileRecords.get(i);
            T match = dbIndex.remove(o);
            if (match == null)
            {
                handleInsert(o);
            }
            else
            {
                compareAndUpdateIfNecessary(match, o);
            }
        }
    }

    protected void processDbRecordsForTermination(List<T> dbRecords)
    {
        for (int i = 0; i < dbRecords.size(); i++)
        {
            T o = dbRecords.get(i);
            T match = fileIndex.remove(o);
            if (match == null)
            {
                handleDeleteOrTerminate(o);
            }
            else
            {
                compareAndUpdateIfNecessary(o, match);
            }
        }
    }

    protected void preprocessMatch(T oldObject, T newObject)
    {
        // for subclass to override.
    }

    /**
     * this is called when the keys for the old and new objects match, but the attributes may or may not match
     * It is called before the comparator decides if the rest of the attributes match.
     * Default implementation does nothing.
     *
     * @param oldObject The existing version of the object (typically from the database)
     * @param newObject The new version of the object (typically from a file or other source)
     */
    protected void handleKeyMatchedBeforeCompare(T oldObject, T newObject)
    {
        // for subclass to override
    }

    /**
     * After the comparator decides that all the attributes match, this method gets called.
     * Default implementation does nothing.
     *
     * @param oldObject The existing version of the object (typically from the database)
     * @param newObject The new version of the object (typically from a file or other source)
     */
    protected void handleIdenticalMatch(T oldObject, T newObject)
    {
        // for subclass to override
    }

    /**
     * If the comparator decides that the (non-key) attributes do no match, this method is called.
     * Default implementation is to call preprocessMatch, followed by executor.addForUpdate
     *
     * @param oldObject The existing version of the object (typically from the database)
     * @param newObject The new version of the object (typically from a file or other source)
     */
    protected void handleUpdate(T oldObject, T newObject)
    {
        this.preprocessMatch(oldObject, newObject);
        executor.addForUpdate(oldObject, newObject);
    }

    /**
     * This method is called with an existing object that is not found in the new source.
     * The default implementation is to call executor.addForTermination
     *
     * @param oldObject The existing version of the object (typically from the database)
     */
    protected void handleDeleteOrTerminate(T oldObject)
    {
        executor.addForTermination(oldObject);
    }

    /**
     * This method is called when a new object is not found in the database.
     * The default implementation is to call executor.addForInsert
     *
     * @param newObject The new version of the object (typically from a file or other source)
     */
    protected void handleInsert(T newObject)
    {
        executor.addForInsert(newObject);
    }

    /**
     * this method is called when the matcher thread is finished processing the database records.
     * Default implementation does nothing.
     */
    protected void handleMatcherFinishedWithDatabaseSide()
    {
        // for subclass to override
    }

    /**
     * this method is called when the matcher thread is finished processing the file records.
     * Default implementation does nothing.
     */
    protected void handleMatcherFinishedWithFileSide()
    {
        // for subclass to override
    }
}


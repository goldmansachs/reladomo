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
import java.util.List;
import java.util.concurrent.Callable;

import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.UnderlyingObjectGetter;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.QueueExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NonUniqueMatcherThread<T extends MithraTransactionalObject> extends AbstractMatcherThread<T>
{
    protected static Logger logger = LoggerFactory.getLogger(NonUniqueMatcherThread.class);

    public NonUniqueMatcherThread(QueueExecutor queueExecutor, Extractor[] extractor)
    {
        super(queueExecutor, extractor);

        ListUnwrapper listUnwrapper = new ListUnwrapper();
        fileIndex.setUnderlyingObjectGetter(listUnwrapper);
        dbIndex.setUnderlyingObjectGetter(listUnwrapper);
    }

    public NonUniqueMatcherThread(QueueExecutor queueExecutor, Extractor[] extractor, int indexSize)
    {
        super(queueExecutor, extractor, indexSize);

        ListUnwrapper listUnwrapper = new ListUnwrapper();
        fileIndex.setUnderlyingObjectGetter(listUnwrapper);
        dbIndex.setUnderlyingObjectGetter(listUnwrapper);
    }

    public Callable constructDbDoneCallable()
    {
        return new DatabaseDoneCallable();
    }

    public Callable constructFileDoneCallable()
    {
        return new FileDoneCallable();
    }

    protected void processRecords(List<T> records, FullUniqueIndex<T> index, FullUniqueIndex<T> matchIndex, boolean fromFile)
    {
        for (int i = 0; i < records.size(); i++)
        {
            T transactionalObject = records.get(i);
            ListWrapper matchingListWrapper = (ListWrapper) matchIndex.getFromData(transactionalObject);
            if (matchingListWrapper == null)
            {
                ListWrapper lw = (ListWrapper) index.getFromData(transactionalObject);
                if (lw == null)
                {
                    lw = new ListWrapper(transactionalObject);
                    index.put(lw);
                }
                else
                {
                    lw.getObjects().add(transactionalObject);
                }
            }
            else
            {
                List objects = matchingListWrapper.getObjects();
                if (objects.size() == 1)
                {
                    matchIndex.remove(matchingListWrapper);
                }
                Object match = objects.remove(objects.size() - 1);
                if (this.comparator.compare(transactionalObject, match) != 0)
                {
                    if (fromFile)
                    {
                        executor.addForUpdate(match, transactionalObject);
                    }
                    else
                    {
                        executor.addForUpdate(transactionalObject, match);
                    }
                }
            }
        }
    }

    protected void processFileRecordsForInsert(List<T> fileRecords)
    {
        for (int i = 0; i < fileRecords.size(); i++)
        {
            T transactionalObject = fileRecords.get(i);
            ListWrapper matchingListWrapper = (ListWrapper) dbIndex.getFromData(transactionalObject);
            if (matchingListWrapper == null)
            {
                executor.addForInsert(transactionalObject);
            }
            else
            {
                List<T> objects = matchingListWrapper.getObjects();
                if (objects.size() == 1)
                {
                    dbIndex.remove(matchingListWrapper);
                }
                Object match = objects.remove(objects.size() - 1);
                if (this.comparator.compare(transactionalObject, match) != 0)
                {
                    executor.addForUpdate(match, transactionalObject);
                }
            }
        }
    }

    protected void processDbRecordsForTermination(List<T> dbRecords)
    {
        for (int i = 0; i < dbRecords.size(); i++)
        {
            MithraTransactionalObject transactionalObject = (MithraTransactionalObject) dbRecords.get(i);

            ListWrapper matchingListWrapper = (ListWrapper) fileIndex.getFromData(transactionalObject);
            if (matchingListWrapper == null)
            {
                executor.addForTermination(transactionalObject);
            }
            else
            {
                List objects = matchingListWrapper.getObjects();
                if (objects.size() == 1)
                {
                    fileIndex.remove(matchingListWrapper);
                }
                Object match = objects.remove(objects.size() - 1);
                if (this.comparator.compare(transactionalObject, match) != 0)
                {
                    executor.addForUpdate(transactionalObject, match);
                }
            }
        }
    }

    private class FileDoneCallable implements Callable
    {
        public Object call() throws Exception
        {
            executor.flushInsert();
            List listOfListWrappers = dbIndex.getAll();
            dbIndex.clear();
            for (int i = 0; i < listOfListWrappers.size(); i++)
            {
                List objects = ((ListWrapper) listOfListWrappers.get(i)).getObjects();
                for (int j = 0; j < objects.size(); j++)
                {
                    executor.addForTermination(objects.get(j));
                }
            }
            executor.flushTermination();
            synchronized (finishedLock)
            {
                fileDone = true;
            }
            return null;
        }
    }

    private class DatabaseDoneCallable implements Callable
    {
        public Object call() throws Exception
        {
            executor.flushTermination();
            List listOfListWrappers = fileIndex.getAll();
            fileIndex.clear();
            for (int i = 0; i < listOfListWrappers.size(); i++)
            {
                List objects = ((ListWrapper) listOfListWrappers.get(i)).getObjects();
                for (int j = 0; j < objects.size(); j++)
                {
                    executor.addForInsert(objects.get(j));
                }
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
            return null;
        }
    }

    private static class ListWrapper
    {
        private List objects = new ArrayList(2);

        public ListWrapper(Object firstObject)
        {
            objects.add(firstObject);
        }

        public Object getFirst()
        {
            return objects.get(0);
        }

        public List getObjects()
        {
            return objects;
        }
    }

    public static class ListUnwrapper implements UnderlyingObjectGetter
    {
        public Object getUnderlyingObject(Object o)
        {
            return ((ListWrapper) o).getFirst();
        }
    }
}

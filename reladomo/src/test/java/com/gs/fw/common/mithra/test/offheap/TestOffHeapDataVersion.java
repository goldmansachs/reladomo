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

package com.gs.fw.common.mithra.test.offheap;

import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.impl.utility.Iterate;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.cache.offheap.FastUnsafeOffHeapDataStorage;
import com.gs.fw.common.mithra.cache.offheap.OffHeapFullDatedCache;
import com.gs.fw.common.mithra.cacheloader.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedNonTransactional;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedNonTransactionalData;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedNonTransactionalFinder;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedNonTransactionalList;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.OperationBasedFilter;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Set;



public class TestOffHeapDataVersion extends MithraTestAbstract
{
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(TestOffHeapDataVersion.class);

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AlarmDatedNonTransactional.class
        };
    }

    /*
     * mithraTestDataDefaultSource.txt
     *
     * class com.gs.fw.common.mithra.test.domain.alarm.AlarmDatedNonTransactional
    id, time, description, processingDateFrom,processingDateTo
    1, "10:30:59.011", "alarm 1", "2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000"
    2, "03:11:23.000", "alarm 2", "2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000"
    3, "14:59:10.043", "alarm 3", "2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000"
    4, null, "null alarm", "2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000"
     */
    public void testRefresh() throws SQLException
    {
        int initialObjectCount = 4;
        int baseObjectId = 10000;
        int testObjectCount = 1000;

        assertObjectCount(initialObjectCount);
        checkDataVersion();

        MithraRuntimeCacheController controller = getMithraRuntimeCacheController(AlarmDatedNonTransactional.class.getName());

//        System.out.println("Inserting initial records");
        Connection connection = this.getConnection();
        Statement stmt = connection.createStatement();
        for (int id = baseObjectId; id < baseObjectId + testObjectCount; id++)
        {
            stmt.executeUpdate("insert into ALARMDATEDNONTRANSACTIONAL (ID, TIME_COL, DESCRIPTION, IN_Z, OUT_Z) values (" + id + ", '01:30:00.0', 'Dummy alarm', '2015-11-19 04:00:00.0', '9999-12-01 23:59:00.0')");
        }

        executeRefresh(controller, Timestamp.valueOf("2015-11-19 00:00:00.0"), Timestamp.valueOf("2015-11-19 03:00:00.0"), 0, 0);
        executeRefresh(controller, Timestamp.valueOf("2015-11-19 03:00:00.0"), Timestamp.valueOf("2015-11-19 06:00:00.0"), 0, testObjectCount);
        executeRefresh(controller, Timestamp.valueOf("2015-11-19 06:00:00.0"), Timestamp.valueOf("2015-11-19 09:00:00.0"), 0, 0);

        assertObjectCount(initialObjectCount + testObjectCount);
        checkDataVersion();

//        System.out.println("Chaining new records");
        for (int id = baseObjectId; id < baseObjectId + testObjectCount; id++)
        {
            stmt.executeUpdate("update ALARMDATEDNONTRANSACTIONAL set OUT_Z = '2015-11-19 12:00:00.0' where ID = " + id + " and IN_Z = '2015-11-19 04:00:00.0'");
            stmt.executeUpdate("insert into ALARMDATEDNONTRANSACTIONAL (ID, TIME_COL, DESCRIPTION, IN_Z, OUT_Z) values (" + id + ", '02:45:00.0', 'Dummy alarm v2', '2015-11-19 12:00:00.0', '9999-12-01 23:59:00.0')");
        }

        executeRefresh(controller, Timestamp.valueOf("2015-11-19 09:00:00.0"), Timestamp.valueOf("2015-11-19 11:00:00.0"), 0, 0);
        executeRefresh(controller, Timestamp.valueOf("2015-11-19 11:00:00.0"), Timestamp.valueOf("2015-11-19 13:00:00.0"), testObjectCount, testObjectCount);

        AlarmDatedNonTransactional alarmDatedNonTransactional = assertObjectCount(initialObjectCount + (testObjectCount * 2));
        checkDataVersion();

//        System.out.println("Dropping date");
        OperationBasedFilter dateDropFilter = new OperationBasedFilter(AlarmDatedNonTransactionalFinder.id().greaterThanEquals(baseObjectId));
        AlarmDatedNonTransactionalFinder.getMithraObjectPortal().getCache().removeAll(dateDropFilter);

        for (int id = baseObjectId + testObjectCount; id < baseObjectId + (testObjectCount * 2); id++)
        {
            stmt.executeUpdate("insert into ALARMDATEDNONTRANSACTIONAL (ID, TIME_COL, DESCRIPTION, IN_Z, OUT_Z) values (" + id + ", '01:30:00.0', 'Dummy alarm', '2015-11-20 04:00:00.0', '9999-12-01 23:59:00.0')");
        }

        executeRefresh(controller, Timestamp.valueOf("2015-11-20 00:00:00.0"), Timestamp.valueOf("2015-11-20 03:00:00.0"), 0, 0);
        executeRefresh(controller, Timestamp.valueOf("2015-11-20 03:00:00.0"), Timestamp.valueOf("2015-11-20 06:00:00.0"), 0, testObjectCount);
        executeRefresh(controller, Timestamp.valueOf("2015-11-20 06:00:00.0"), Timestamp.valueOf("2015-11-20 09:00:00.0"), 0, 0);

        AlarmDatedNonTransactionalData.AlarmDatedNonTransactionalDataOffHeap data = (AlarmDatedNonTransactionalData.AlarmDatedNonTransactionalDataOffHeap) alarmDatedNonTransactional.zGetCurrentData();
        logger.warn("Wait for removed objects to get evicted");
        {
            while (((FastUnsafeOffHeapDataStorage) data.zGetStorage()).zGetStackSizeForTest() < testObjectCount)
            {
                logger.warn("Perform full GC and re-attempt eviction of removed objects");
                System.gc();
                ((OffHeapFullDatedCache) AlarmDatedNonTransactionalFinder.getMithraObjectPortal().getCache()).evictCollectedReferences();
                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException e)
                {
                    // Ignore
                }
            }
        }

        assertObjectCount(initialObjectCount + testObjectCount);
        checkDataVersion();

        logger.warn("Chaining second day new records");
        for (int id = baseObjectId + testObjectCount; id < baseObjectId + (testObjectCount * 2); id++)
        {
            stmt.executeUpdate("update ALARMDATEDNONTRANSACTIONAL set OUT_Z = '2015-11-20 12:00:00.0' where ID = " + id + " and IN_Z = '2015-11-20 04:00:00.0'");
            stmt.executeUpdate("insert into ALARMDATEDNONTRANSACTIONAL (ID, TIME_COL, DESCRIPTION, IN_Z, OUT_Z) values (" + id + ", '02:45:00.0', 'Dummy alarm v2', '2015-11-20 12:00:00.0', '9999-12-01 23:59:00.0')");
        }

        executeRefresh(controller, Timestamp.valueOf("2015-11-20 09:00:00.0"), Timestamp.valueOf("2015-11-20 11:00:00.0"), 0, 0);
        executeRefresh(controller, Timestamp.valueOf("2015-11-20 11:00:00.0"), Timestamp.valueOf("2015-11-20 13:00:00.0"), testObjectCount, testObjectCount);

        assertObjectCount(initialObjectCount + (testObjectCount * 2));
        checkDataVersion(); // this is the check which will fail if dataVersion is not initialised to zero on every new object

        connection.close();
    }

    private AlarmDatedNonTransactional assertObjectCount(int expectedCount)
    {
        AlarmDatedNonTransactionalList list = AlarmDatedNonTransactionalFinder.findMany(AlarmDatedNonTransactionalFinder.processingDate().equalsEdgePoint());
        assertEquals(expectedCount, list.size());
        return list.get(0);
    }

    private ExternalQueueThreadExecutor createExecutor()
    {
        ExternalQueueThreadExecutor.ExceptionHandler exceptionHandler = new ExternalQueueThreadExecutor.ExceptionHandler()
        {
            @Override
            public void handleException(Runnable task, Throwable exception)
            {
                exception.printStackTrace();
                throw new RuntimeException(exception);
            }
        };

        DualCapacityBlockingQueue<Runnable> queue = new DualCapacityBlockingQueue<Runnable>(10);
        return new ExternalQueueThreadExecutor(queue, exceptionHandler, "pool", 2);
    }

    private void checkDataVersion()
    {
        AlarmDatedNonTransactionalList orders = AlarmDatedNonTransactionalFinder.findMany(AlarmDatedNonTransactionalFinder.processingDate().equalsEdgePoint());
        for(AlarmDatedNonTransactional order : orders)
        {
            byte version = order.zGetCurrentData().zGetDataVersion();
            if (version != 0 && version != 1 && version != 2)
            {
                fail("Found an invalid data version on a live object: " + version);
            }
        }
    }

    private MithraRuntimeCacheController getMithraRuntimeCacheController(final String mithraClassName)
    {
        Set<MithraRuntimeCacheController> controllers = MithraManagerProvider.getMithraManager().getRuntimeCacheControllerSet();
        return Iterate.select(controllers, new Predicate<MithraRuntimeCacheController>()
        {
            @Override
            public boolean accept(MithraRuntimeCacheController each)
            {
                return each.getClassName().equals(mithraClassName);
            }
        }).iterator().next();
    }

    private void executeRefresh(MithraRuntimeCacheController controller, Timestamp startTime, Timestamp endTime, int expectedOutzCount, int expectedInzCount)
    {
        DateCluster dateCluster = null;

        Operation outzOp = AlarmDatedNonTransactionalFinder.processingDate().equalsEdgePoint().and(AlarmDatedNonTransactionalFinder.processingDateTo().greaterThan(startTime).and(AlarmDatedNonTransactionalFinder.processingDateTo().lessThan(endTime)));
        Operation inzOp = AlarmDatedNonTransactionalFinder.processingDate().equalsEdgePoint().and(AlarmDatedNonTransactionalFinder.processingDateFrom().greaterThan(startTime).and(AlarmDatedNonTransactionalFinder.processingDateFrom().lessThan(endTime)));

        PostLoadFilterBuilder postLoadFilterBuilder = new PostLoadFilterBuilder(null, null, AlarmDatedNonTransactionalFinder.getFinderInstance(), null);

        LoadingTaskImpl outzTask = new LoadingTaskImpl(controller, new LoadOperationBuilder(outzOp, null, dateCluster, AlarmDatedNonTransactionalFinder.getFinderInstance()), postLoadFilterBuilder, dateCluster);
        LoadingTaskImpl inzTask = new LoadingTaskImpl(controller, new LoadOperationBuilder(inzOp, null, dateCluster, AlarmDatedNonTransactionalFinder.getFinderInstance()), postLoadFilterBuilder, dateCluster);

//        System.out.println("Executing refresh for (" + startTime + ", " + endTime + ")");

        int actualOutzCount = outzTask.load();
        int actualInzCount = inzTask.load();

//        System.out.println("Object count loaded by OUT_Z refresh = " + actualOutzCount);
//        System.out.println("Object count loaded by IN_Z refresh = " + actualInzCount);

        assertEquals(expectedOutzCount, actualOutzCount);
        assertEquals(expectedInzCount, actualInzCount);
    }

    private static class TaskRunner implements Runnable
    {
        private LoadingTaskImpl task;
        private int loadedObjectCount;
        private boolean finished;

        public TaskRunner(LoadingTaskImpl task)
        {
            this.task = task;
        }

        @Override
        public void run()
        {
            loadedObjectCount = task.load();
            finished = true;
        }

        public int getLoadedObjectCount()
        {
            return loadedObjectCount;
        }

        public boolean isFinished()
        {
            return finished;
        }
    }
}

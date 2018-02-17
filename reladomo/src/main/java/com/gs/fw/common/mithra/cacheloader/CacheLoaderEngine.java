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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.fw.common.mithra.database.SyslogChecker;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CacheLoaderEngine implements ExternalQueueThreadExecutor.ExceptionHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheLoaderEngine.class);

    private final Map<String, LoadingTaskThreadPoolHolder> loadingTaskTreadPools = UnifiedMap.newMap();

    private boolean initCompleted = false;

    /**
     * count is the general indicator of how much still need to be loaded.
     * it is changed by +1 when new LoadingTaskRunner created and by -1 when LoadingTaskRunner finishes
     * it is changed by +1 when object added to the queue stripe on the DepenendentCursor and and by -stripe.size() when stripe is dequeued
     * is it changed by +1 when object is added to the dependent key index and by -list.size() when loading task is created from the index.
     */
    private long count = 0L;
    private boolean readyToScheduleTasks = true;

    private final List<LoadingTaskRunner> loadingTaskRunners = FastList.newList();
    private List<LoadingTaskMonitor> taskMonitors; // data collected only when MonitoredConfig is switched to capture details
    private volatile boolean stopped = false;

    private List<RuntimeException> exceptions = FastList.<RuntimeException>newList().asSynchronized();

    private ConfigValues configValues;

    /**
     * @threadsafe executed during engine construction
     */
    public LoadingTaskThreadPoolHolder getOrCreateThreadPool(String name)
    {
        LoadingTaskThreadPoolHolder loadingTaskThreadPoolHolder = this.loadingTaskTreadPools.get(name);
        if (loadingTaskThreadPoolHolder == null)
        {
            if (initCompleted)
            {
                throw new RuntimeException();
            }
            loadingTaskThreadPoolHolder = new LoadingTaskThreadPoolHolder(this, name, this.configValues.getThreadsPerDbServer(),
                    new SyslogChecker(this.configValues.getSyslogCheckThreshold(), this.configValues.getSyslogCheckMaxWait()));
            loadingTaskTreadPools.put(name, loadingTaskThreadPoolHolder);
        }

        return loadingTaskThreadPoolHolder;
    }

    public void addTaskToLoadAndSetupThreadPool(LoadingTaskRunner loadingTaskRunner)
    {
        final String threadPoolName = loadingTaskRunner.getThreadPoolName();
        SyslogChecker syslogChecker = this.getOrCreateThreadPool(threadPoolName).getSyslogChecker();

        ((LoadingTaskImpl) loadingTaskRunner.getLoadingTask()).setSyslogChecker(syslogChecker);
        this.addTaskToLoad(loadingTaskRunner);
    }

    protected void addTaskToLoad(LoadingTaskRunner loadingTaskRunner)
    {
        this.checkEngine();

        loadingTaskRunner.getLoadingTaskMonitor().setConfigValues(this.configValues);
        synchronized (this.loadingTaskRunners)
        {
            loadingTaskRunners.add(loadingTaskRunner);
            if (configValues.isCaptureLoadingTaskDetails())
            {
                this.taskMonitors.add(loadingTaskRunner.getLoadingTaskMonitor());
            }
        }
    }

    private void checkExceptions()
    {
        if (!this.exceptions.isEmpty())
        {
            for (Exception each : this.exceptions)
            {
                LOGGER.error("error reported", each);
            }
            throw new RuntimeException("Cache load aborted due to following exception(s): " + this.exceptions + " check log for stack traces", this.exceptions.get(0));
        }
    }

    public void shutdown()
    {
        LOGGER.debug("Initiating shutdown sequence.");
        this.stopped = true;
        for (LoadingTaskThreadPoolHolder each : this.loadingTaskTreadPools.values())
        {
            each.getThreadPool().shutdown();
        }
        LOGGER.debug("Shutdown completed.");
    }

    public void checkEngine()
    {
        if (this.stopped) throw new RuntimeException("The engine is stopped.");
    }

    public synchronized void changeKeyIndexCount(long diff)
    {
        this.count += diff;
    }

    /**
     * The dependent loader logic goes like this:
     * <p/>
     * 1.	In cursor thread put items in stripes on queue and increment the counter.
     * 2.	In cursor thread finish cursor, decrement count by one completed task and signal that task completed.
     * 3.	In consumer thread pick up stripes from the queue, check if items are not duplicates with FullUniqueIndex and add them to index if necessary
     * 4.	Increment count by number of added items
     * 5.	Decrement count by number of items removed from the stripe.
     * <p/>
     * The problem happens when #2 is the end of the last scheduled task. At this point we fire notify.
     * The count is still positive because we added something to the stripes. After this we pick items from
     * the stripes and check on the FullUniqueIndex, but coincidentally the index already has all the elements
     * and these are duplicates. So we add 0 at #4 and deduct all items from the stripe at #5. The count=0 but we do
     * not have any tasks to execute and nobody will notify the engine to get out of wait.
     */
    public synchronized void changeStripedCount(long diff)
    {
        this.count += diff;

        if (this.count == 0)
        {
            this.signalTaskCompleted();
        }
    }

    public synchronized void changeTaskCount(long diff)
    {
        this.count += diff;
    }

    private void startThreadPools()
    {
        for (LoadingTaskThreadPoolHolder each : this.loadingTaskTreadPools.values())
        {
            each.getThreadPool().awaitForAbandonedThreads();
        }

        for (LoadingTaskThreadPoolHolder each : this.loadingTaskTreadPools.values())
        {
            each.getThreadPool().startThreads();
        }
        initCompleted = true;
    }

    public void waitUntilAllTasksCompleted()
    {
        this.startThreadPools();

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("started waitUntilAllTasksCompleted");
        }

        try
        {
            while (this.waitUntilReadyAndCheckCount())
            {
                this.checkExceptions();
                this.scheduleRemainingTasks();
            }
        }
        finally
        {
            this.shutdown();
            this.checkExceptions();
        }

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("finished waitUntilAllTasksCompleted");
        }
    }

    private void scheduleRemainingTasks()
    {
        synchronized (this.loadingTaskRunners)
        {
            for (int i = this.loadingTaskRunners.size() - 1; i >= 0; i--)
            {
                LoadingTaskRunner task = this.loadingTaskRunners.get(i);
                if (task.getState() == LoadingTaskRunner.State.COMPLETED || task.getState() == LoadingTaskRunner.State.QUEUED)
                {
                    final int lastIndex = this.loadingTaskRunners.size() - 1;
                    this.loadingTaskRunners.set(i, this.loadingTaskRunners.get(lastIndex));
                    this.loadingTaskRunners.remove(lastIndex);
                }
                else
                {
                    task.advance(this.loadingTaskTreadPools.get(task.getThreadPoolName()));
                }
            }
        }
    }

    protected synchronized void signalTaskCompleted()
    {
        this.readyToScheduleTasks = true;
        this.notify();
    }

    private synchronized boolean waitUntilReadyAndCheckCount()
    {
        while (!this.readyToScheduleTasks)
        {
            try
            {
                this.wait();
                this.readyToScheduleTasks = false;
                return count != 0;
            }
            catch (InterruptedException ie)
            {
                // continue;
            }
        }
        this.readyToScheduleTasks = false;
        return count != 0;
    }

    public List<LoadingTaskThreadPoolMonitor> getThreadPoolMonitors()
    {
        List<LoadingTaskThreadPoolMonitor> monitors = FastList.newList();
        for (Map.Entry<String, LoadingTaskThreadPoolHolder> entry : this.loadingTaskTreadPools.entrySet())
        {
            LoadingTaskThreadPoolMonitor state = new LoadingTaskThreadPoolMonitor(entry.getKey());
            entry.getValue().updateMonitor(state);
            monitors.add(state);
        }

        return monitors;
    }

    public void setConfigValues(ConfigValues configValues)
    {
        this.configValues = configValues;
        if (this.configValues.isCaptureLoadingTaskDetails())
        {
            this.taskMonitors = FastList.newList();
        }
    }

    /**
     * @return list of the LoadingTaskMonitor if MonitoredConfig set to capture task details. Return null otherwise
     */
    public List<LoadingTaskMonitor> getTaskMonitors()
    {
        return taskMonitors;
    }

    public SyslogChecker getTempdbSyslogCheckerForConnectionPool(String threadPoolName)
    {
        return this.getOrCreateThreadPool(threadPoolName).getSyslogChecker();
    }

    public void handleException(Runnable task, Throwable exception)
    {
        if (!stopped) // nobody wants to hear from zombie
        {
            String aString = task.toString() + " failed";
            LOGGER.error(aString, exception);
            RuntimeException re = new RuntimeException(aString, exception);
            this.exceptions.add(re);
            this.signalTaskCompleted();
        }
    }

    public long count()
    {
        return this.count;
    }
}

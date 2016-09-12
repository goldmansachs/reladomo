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

package com.gs.fw.common.mithra.test.cacheloader;

import com.gs.fw.common.mithra.cacheloader.*;
import junit.framework.TestCase;


public class LoadingTaskMonitorTest extends TestCase
{
    public void testMonitoring()
    {
        TestLoadingTask loadingTask = new TestLoadingTask();
        LoadingTaskMonitor monitor = new LoadingTaskMonitor(loadingTask, "t3000");
        monitor.setConfigValues(new ConfigValues(1, 1, true, 10, 45.0, 1200));
        monitor.startMonitoring(LoadingTaskRunner.State.PROCESSING);
        monitor.finishMonitoring(loadingTask, 1000, LoadingTaskRunner.State.PROCESSING);
        assertTrue(System.currentTimeMillis() - monitor.getStartTime() < 1000 * 60 * 1000);
        assertTrue(System.currentTimeMillis() >= monitor.getFinishTime());
        assertTrue(monitor.getFinishTime() - monitor.getStartTime() >= 0);
    }

    private class TestLoadingTask implements LoadingTask
    {
        public int load()
        {
            return 0;
        }

        public String getClassName()
        {
            return "com.gs.domain.Trade";
        }

        public Object getSourceAttribute()
        {
            return null;
        }

        public String getOperationAsString()
        {
            return null;
        }

        public void addDependentThread(DependentKeyIndex processor)
        {
        }

        @Override
        public DateCluster getDateCluster()
        {
            return null;
        }
    }
}

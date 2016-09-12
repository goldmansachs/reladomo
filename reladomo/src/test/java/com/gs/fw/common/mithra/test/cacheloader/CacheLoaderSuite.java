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


import junit.framework.Test;
import junit.framework.TestSuite;

public class CacheLoaderSuite extends TestSuite
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite();

        suite.addTestSuite(AbstractLoaderFactoryTest.class);
        suite.addTestSuite(DateLoadClusterTest.class);
        suite.addTestSuite(CacheLoaderEngineTest.class);
        suite.addTestSuite(CacheLoaderManagerTest.class);
        suite.addTestSuite(CacheLoaderManagerProcessingOnlyTest.class);
        suite.addTestSuite(CacheLoaderConfigTest.class);
        suite.addTestSuite(DependentSingleKeyIndexTest.class);
        suite.addTestSuite(DependentLoaderFactoryTest.class);
        suite.addTestSuite(LoadingTaskMonitorTest.class);
        suite.addTestSuite(BusinessDateFilterTest.class);
        suite.addTestSuite(CacheIndexBasedFilterTest.class);
        suite.addTestSuite(IOTaskThreadPoolWithCpuTaskConveyorTest.class);
        suite.addTestSuite(ExternalQueueThreadExecutorTest.class);

        suite.addTestSuite(FullyMilestonedTopLevelLoaderFactoryTest.class);
        suite.addTestSuite(ProcessingDateMilestonedTopLevelLoaderFactoryTest.class);

        return suite;
    }
}

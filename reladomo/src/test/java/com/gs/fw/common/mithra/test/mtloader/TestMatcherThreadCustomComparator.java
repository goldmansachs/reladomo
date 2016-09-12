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

package com.gs.fw.common.mithra.test.mtloader;

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.mtloader.AbstractMatcherThread;
import com.gs.fw.common.mithra.mtloader.MatcherThread;
import com.gs.fw.common.mithra.util.QueueExecutor;


public class TestMatcherThreadCustomComparator extends TestMatcherThread
{
    @Override
    protected AbstractMatcherThread getMatcherThread(QueueExecutor bitemporalOrderExecutor, Extractor[] bitemporalOrderExtractor)
    {
        MatcherThread matcherThread = new MatcherThread(bitemporalOrderExecutor, bitemporalOrderExtractor, INDEX_SIZE);
        matcherThread.setComparator(COMPARATOR);
        return matcherThread;
    }

    @Override
    public void testMtFeedLoad()
    {
        // Different assertions
        QueueExecutor executor = loadData();
        assertResultsCustomComparator(executor);
    }

    @Override
    public void testMtFeedLoad_noDbThread()
    {
        // Different assertions
        QueueExecutor executor = loadData_noDbThread();
        assertResultsCustomComparator(executor);
    }
}

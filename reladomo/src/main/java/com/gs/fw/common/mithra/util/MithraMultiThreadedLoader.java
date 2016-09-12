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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;

import java.util.List;


/**
 * The use of this class is completely optional. A MithraList will normally resolve itself on demand.
 * This class provides a mechanism to resolve multiple lists simultaneously in multiple threads.
 * Any exceptions are properly propagated back from all threads.
 * When in a transaction, this class does not multi-thread the operation to comply with the
 * one-transaction to one-thread model.
 */
public class MithraMultiThreadedLoader
{

    private ThreadConservingExecutor executor;

    private Logger logger = LoggerFactory.getLogger(MithraMultiThreadedLoader.class.getName());
    private int maxThreads;

    public MithraMultiThreadedLoader(int maxThreads)
    {
        this.maxThreads = maxThreads;
    }

    public void loadMultipleLists(List listOfLists)
    {
        if (listOfLists.size() > 0)
        {
            if (listOfLists.size() == 1 || MithraManagerProvider.getMithraManager().isInTransaction())
            {
                // don't do anything. let the lists be resolved as they are used.
            }
            else
            {
                loadInMultipleThreads(listOfLists);
            }
        }
    }

    private void loadInMultipleThreads(final List<MithraList> listOfLists)
    {
        this.executor = new ThreadConservingExecutor(maxThreads);
        for(int i=0;i<listOfLists.size();i++)
        {
            this.executor.submit(new LoaderRunnable(listOfLists.get(i)));
        }
        this.executor.finish();
    }

    public Logger getLogger()
    {
        return this.logger;
    }

    public void setLogger(Logger newLogger)
    {
        this.logger = newLogger;
    }

    private static class LoaderRunnable
    implements Runnable
    {
        private MithraList listToLoad;

        public LoaderRunnable(MithraList listToLoad)
        {
            this.listToLoad = listToLoad;
        }

        public void run()
        {
            this.listToLoad.forceResolve();
        }
    }

}

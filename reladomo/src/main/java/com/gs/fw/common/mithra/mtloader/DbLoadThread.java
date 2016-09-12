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

import com.gs.fw.common.mithra.finder.AbstractAtomicOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DbLoadThread extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(DbLoadThread.class);

    private static final int BUCKET_SIZE = 100;

    private DelegatingList delegatingList;
    private AbstractMatcherThread matcherThread;
    private AbstractAtomicOperation filter;

    private long lastPrintout;

    public DbLoadThread(DelegatingList delegatingList, Operation filter, AbstractMatcherThread matcherThread)
    {
        this.delegatingList = delegatingList;
        this.matcherThread = matcherThread;
        this.filter = (AbstractAtomicOperation) filter;
        this.setName("DB load thread");
    }

    public void run()
    {
        long start = System.currentTimeMillis();
        final ArrayList[] temp = new ArrayList[1];
        temp[0] = new ArrayList(BUCKET_SIZE);
        logger.info("Starting to DB Thread");
        try
        {
            delegatingList.forEachWithCursor(new DoWhileProcedure()
            {
                int count = 0;

                public boolean execute(Object o)
                {
                    if (filter == null || filter.matches(o))
                    {
                        temp[0].add(o);
                        if (temp[0].size() == BUCKET_SIZE)
                        {
                            try
                            {
                                matcherThread.addDbRecords(temp[0]);
                            }
                            catch (AbortException e)
                            {
                                return false;
                            }
                            temp[0] = new ArrayList(BUCKET_SIZE);
                        }
                        count++;
                        if (System.currentTimeMillis() - lastPrintout >= 30000)
                        {
                            lastPrintout = System.currentTimeMillis();
                            logger.info("Read " + count + " objects from DB");
                        }
                    }
                    return true;
                }
            });
        }
        catch (Throwable e)
        {
            matcherThread.setMustAbort(e);
        }
        if (temp[0].size() > 0)
        {
            logger.info("Read final " + temp[0].size() + " objects from DB");
            try
            {
                matcherThread.addDbRecords(temp[0]);
            }
            catch (AbortException e)
            {
                // too bad, it was on the last batch!
            }
        }
        matcherThread.setDbDone();
        logger.info("took " + (System.currentTimeMillis() - start));
    }
}


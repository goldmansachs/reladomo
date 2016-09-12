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

import com.gs.fw.common.mithra.MithraTransactionalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PlainInputThread extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(PlainInputThread.class);

    private InputLoader inputLoader;
    private AbstractMatcherThread matcherThread;
    private long lastReportTime = 0;
    private static final long MIN_REPORT_PERIOD = 30000;

    public PlainInputThread(InputLoader<MithraTransactionalObject> inputLoader, AbstractMatcherThread matcherThread)
    {
        this.inputLoader = inputLoader;
        this.matcherThread = matcherThread;
        this.setName("Input thread");
    }

    @Override
    public void run()
    {
        lastReportTime = System.currentTimeMillis();
        logger.info("Starting to read input");
        int count = 0;
        try
        {
            while (!inputLoader.isFileParsingComplete())
            {
                //get parsedObjects from the processModel
                List<? extends MithraTransactionalObject> parsedObjectList = inputLoader.getNextParsedObjectList();
                count += parsedObjectList.size();
                reportCount(count);
                try
                {
                    matcherThread.addFileRecords(transform(parsedObjectList));
                }
                catch (AbortException e)
                {
                    break;
                }
            }
        }
        catch (Throwable e)
        {
            matcherThread.setMustAbort(e);
        }
        matcherThread.setFileDone();
        logger.info("Read " + count + " total objects from feed");
    }

    private void reportCount(int count)
    {
        final long now = System.currentTimeMillis();
        if (now > lastReportTime + MIN_REPORT_PERIOD)
        {
            logger.info("Read " + count + " objects from feed");
            lastReportTime = now;
        }
    }

    public List<? extends MithraTransactionalObject> transform(List<? extends MithraTransactionalObject> parsedObjectList)
    {
        return parsedObjectList; // subclass can override
    }
}

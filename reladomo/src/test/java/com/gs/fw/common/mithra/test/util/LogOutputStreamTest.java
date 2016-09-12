
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

package com.gs.fw.common.mithra.test.util;

import java.io.PrintWriter;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.test.MockLogger;
import com.gs.fw.common.mithra.util.execute.LogOutputStream;


public class LogOutputStreamTest extends TestCase
{

    private static int LINE_COUNT = 2;

    public void testLogDebugOutputStream() throws Exception
    {
        // Test with the logging turned on
        MockLogger logger = new MockLogger();
        logger.setDebugEnabled(true);
        logger.setExpectedLogCalls(LINE_COUNT);

        this.doTest(logger, LogOutputStream.logDebug(logger));

        // Turn off the logging
        MockLogger disabledLogger = new MockLogger();
        disabledLogger.setDebugEnabled(false);

        this.doTest(disabledLogger, LogOutputStream.logDebug(disabledLogger));
    }

    public void testLogInfoOutputStream() throws Exception
    {
        // Test with the logging turned on
        MockLogger logger = new MockLogger();
        logger.setDebugEnabled(true);
        logger.setExpectedLogCalls(LINE_COUNT);

        this.doTest(logger, LogOutputStream.logDebug(logger));

        // Turn off the logging
        MockLogger disabledLogger = new MockLogger();
        disabledLogger.setDebugEnabled(false);

        this.doTest(disabledLogger, LogOutputStream.logDebug(disabledLogger));
    }

    public void testLogWarnOutputStream() throws Exception
    {
        // Test with the logging turned on
        MockLogger logger = new MockLogger();
        logger.setDebugEnabled(true);
        logger.setExpectedLogCalls(LINE_COUNT);

        this.doTest(logger, LogOutputStream.logDebug(logger));

        // Turn off the logging
        MockLogger disabledLogger = new MockLogger();
        disabledLogger.setDebugEnabled(false);

        this.doTest(disabledLogger, LogOutputStream.logDebug(disabledLogger));
    }

    public void testLogErrorOutputStream() throws Exception
    {
        // Test with the logging turned on
        MockLogger logger = new MockLogger();
        logger.setDebugEnabled(true);
        logger.setExpectedLogCalls(LINE_COUNT);

        this.doTest(logger, LogOutputStream.logDebug(logger));

        // Turn off the logging
        MockLogger disabledLogger = new MockLogger();
        disabledLogger.setDebugEnabled(false);

        this.doTest(disabledLogger, LogOutputStream.logDebug(disabledLogger));
    }

    private void doTest(MockLogger logger, LogOutputStream stream)
    {
        PrintWriter writer = new PrintWriter(stream, true);
        writer.print("Something");
        writer.println("Else");
        writer.print("And again");
        writer.println();

        logger.verify();
    }

}

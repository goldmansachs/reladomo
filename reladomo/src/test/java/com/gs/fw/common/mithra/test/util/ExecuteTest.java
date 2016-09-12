
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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import com.mockobjects.ExpectationCounter;
import com.mockobjects.Verifiable;
import junit.framework.TestCase;

import com.gs.fw.common.mithra.util.execute.Execute;
import com.gs.fw.common.mithra.util.execute.StreamHandler;


public class ExecuteTest extends TestCase
{

    public void testExecuteEcho() throws Exception
    {
        MockStreamHandler streamHandler = new MockStreamHandler();
        streamHandler.setExpectedStartCalls(1);
        streamHandler.setExpectedStopCalls(1);

        Execute execute = new Execute();
        execute.setCommand(Arrays.asList(new String[] {"hostname"}));
        execute.setTerminateOnJvmExit(true);
        execute.setStreamHandler(streamHandler);

        int exitCode = execute.execute();
        assertEquals("Wrong exit code from echo.", 0, exitCode);

        streamHandler.verify();
    }

    private static class MockStreamHandler implements StreamHandler, Verifiable
    {
        /** Expectation counter for <code>stop</code> */
        private ExpectationCounter stopCounter = new ExpectationCounter("stop");

        /** Expectation counter for <code>start</code> */
        private ExpectationCounter startCounter = new ExpectationCounter("start");

        /**
         * Sets the number of expected calls to <code>start</code>
         * @param count The number of expected calls.
         */
        public void setExpectedStartCalls(int count)
        {
            this.startCounter.setExpected(count);
        }

        /**
         * Sets the number of expected calls to <code>stop</code>
         * @param count The number of expected calls.
         */
        public void setExpectedStopCalls(int count)
        {
            this.stopCounter.setExpected(count);
        }

        public void setErrorStream(InputStream errorStream)
        {
        }

        public void setOutputStream(OutputStream outputStream)
        {
        }

        public void setInputStream(InputStream inputStream)
        {
        }

        public void start( )
        {
            this.startCounter.inc();
        }

        public void stop()
        {
            this.stopCounter.inc();
        }

        public void verify()
        {
            this.startCounter.verify();
            this.stopCounter.verify();
        }
    }
}

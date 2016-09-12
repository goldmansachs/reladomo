
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.util.execute.PumpStreamHandler;


public class PumpStreamHandlerTest extends TestCase
{

    public void testPumpStreamHandler() throws Exception
    {
        ByteArrayOutputStream stderrOut = new ByteArrayOutputStream();
        ByteArrayOutputStream stdoutOut = new ByteArrayOutputStream();
        ByteArrayInputStream stderrIn = new ByteArrayInputStream("Error".getBytes());
        ByteArrayInputStream stdoutIn = new ByteArrayInputStream("Out".getBytes());

        PumpStreamHandler handler = new PumpStreamHandler(stdoutOut, stderrOut);
        handler.setErrorStream(stderrIn);
        handler.setInputStream(stdoutIn);

        handler.start();

        Thread.sleep(1000);

        handler.stop();

        assertEquals("Wrong value copied from stdout", "Out", stdoutOut.toString());
    }
}

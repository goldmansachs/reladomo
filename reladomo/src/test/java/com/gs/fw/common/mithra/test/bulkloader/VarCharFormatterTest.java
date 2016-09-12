
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

package com.gs.fw.common.mithra.test.bulkloader;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.bulkloader.VarCharFormatter;
import com.gs.fw.common.mithra.test.MockLogger;


public class VarCharFormatterTest extends TestCase
{

    public void testFormatChar() throws Exception
    {
        VarCharFormatter formatter = new VarCharFormatter(null, "test", 10);
        String formattedValue = formatter.format('c');

        assertEquals("char format is incorrect.", "c", formattedValue);
    }

    public void testFormatNull() throws Exception
    {
        VarCharFormatter formatter = new VarCharFormatter(null, "test", 10);
        String formattedValue = formatter.format(null);

        assertEquals("null format is incorrect.", "", formattedValue);
    }

    public void testFormatString() throws Exception
    {
        MockLogger logger = new MockLogger();
        logger.setWarnEnabled(true);
        logger.setExpectedLogCalls(1);

        VarCharFormatter formatter = new VarCharFormatter(logger, "test", 10);

        String underflowValue = formatter.format("underflow");
        assertEquals("underflow format is incorrect.", "underflow", underflowValue);

        String overflowValue = formatter.format("some overflow");
        assertEquals("underflow format is incorrect.", "some overf", overflowValue);

        logger.verify();
    }
}

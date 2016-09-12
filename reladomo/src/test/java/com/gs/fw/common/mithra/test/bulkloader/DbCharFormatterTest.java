
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

import com.gs.fw.common.mithra.bulkloader.DbCharFormatter;
import com.gs.fw.common.mithra.test.MockLogger;


public class DbCharFormatterTest extends TestCase
{

    public void testFormatCharPadded() throws Exception
    {
        DbCharFormatter formatter = new DbCharFormatter(null, "test", 5);
        String formattedValue = formatter.format('c');

        assertEquals("Wrong formatted value.", "c    ", formattedValue);
    }

    public void testFormatCharLengthOne() throws Exception
    {
        DbCharFormatter formatter = new DbCharFormatter(null, "test", 1);
        String formattedValue = formatter.format('c');

        assertEquals("Wrong formatted value.", "c", formattedValue);
    }

    public void testFormatCharacter() throws Exception
    {
        DbCharFormatter formatter = new DbCharFormatter(null, "test", 1);
        String formattedValue = formatter.format(new Character('c'));

        assertEquals("Wrong formatted value.", "c", formattedValue);
    }

    public void testFormatStringPadded() throws Exception
    {
        DbCharFormatter formatter = new DbCharFormatter(null, "test", 5);
        String formattedValue = formatter.format("some");

        assertEquals("Wrong formatted value.", "some ", formattedValue);
    }

    public void testFormatStringOverflow() throws Exception
    {
        MockLogger logger = new MockLogger();
        logger.setWarnEnabled(true);
        logger.setExpectedLogCalls(1);

        DbCharFormatter formatter = new DbCharFormatter(logger, "test", 5);
        String formattedValue = formatter.format("something");

        assertEquals("Wrong formatted value.", "somet", formattedValue);

        logger.verify();
    }
}

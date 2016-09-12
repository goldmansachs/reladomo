
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

package com.gs.fw.common.mithra.test.attribute;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.attribute.ToStringFormatter;


public class ToStringFormatterTest extends TestCase
{

    public void testFormatNullString() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong null formatted value", "", formatter.format(null));
    }

    public void testFormatObject() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong null formatted value", "Something", formatter.format(new Object()
        {
            public String toString()
            {
                return "Something";
            }
        }));
    }

    public void testFormatBoolean() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for true", "true", formatter.format(true));
        assertEquals("Wrong value for false", "false", formatter.format(false));
    }

    public void testFormatByte() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for byte", "10", formatter.format((byte) 10));
    }

    public void testFormatChar() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for char", "c", formatter.format('c'));
    }

    public void testFormatDouble() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for double", "10.1", formatter.format(10.1d));
    }

    public void testFormatFloat() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for float", "12.23", formatter.format(12.23f));
    }

    public void testFormatInt() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for int", "5", formatter.format(5));
    }

    public void testFormatLong() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for long", "7", formatter.format(7l));
    }

    public void testFormatShort() throws Exception
    {
        ToStringFormatter formatter = new ToStringFormatter();
        assertEquals("Wrong value for short", "2", formatter.format((short) 2));
    }
}

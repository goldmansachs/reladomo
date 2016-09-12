
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

import com.gs.fw.common.mithra.bulkloader.DecimalPlaceFormatter;


public class DecimalPlaceFormatterTest extends TestCase
{

    public void testFormatter() throws Exception
    {
        DecimalPlaceFormatter formatter = new DecimalPlaceFormatter(2);

        String doubleValue = formatter.format(0.02d);
        assertEquals("Wrong double value.", "0.02", doubleValue);

        String floatValue = formatter.format(0.02f);
        assertEquals("Wrong float value.", "0.02", floatValue);

        String doubleRoundUpValue = formatter.format(0.006d);
        assertEquals("Wrong double value rounded up.", "0.01", doubleRoundUpValue);

        String floatRoundUpValue = formatter.format(0.006f);
        assertEquals("Wrong float value rounded up.", "0.01", floatRoundUpValue);

        String doubleRoundDownValue = formatter.format(0.014d);
        assertEquals("Wrong double value rounded down.", "0.01", doubleRoundDownValue);

        String floatRoundDownValue = formatter.format(0.014f);
        assertEquals("Wrong float value rounded down.", "0.01", floatRoundDownValue);
    }
}


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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.attribute.Attribute;

import java.text.Format;
import java.util.List;
public class MithraDelimitedTestDataParser extends com.gs.fw.common.mithra.util.fileparser.MithraDelimitedDataParser
{
    // This subclass was created to maintain backwards compatibility of the public API which was moved into a non-test package (via the superclass)

    public MithraDelimitedTestDataParser(String bcpFilename, String delimiter, List<Attribute> attributes, String dateFormatString)
    {
        super(bcpFilename, delimiter, attributes, dateFormatString);
    }

    public MithraDelimitedTestDataParser(String bcpFilename, String delimiter, List<Attribute> attributes, Format dateFormat)
    {
        super(bcpFilename, delimiter, attributes, dateFormat);
    }
}

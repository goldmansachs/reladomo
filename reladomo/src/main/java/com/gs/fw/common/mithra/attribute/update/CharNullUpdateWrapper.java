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

package com.gs.fw.common.mithra.attribute.update;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.CharExtractor;

import java.sql.Types;



public class CharNullUpdateWrapper extends PrimitiveNullUpdateWrapper implements CharExtractor
{

    public CharNullUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate)
    {
        super(attribute, dataToUpdate);
    }

    public CharNullUpdateWrapper()
    {
    }

    public int getSqlType()
    {
        return Types.CHAR;
    }

    public char charValueOf(Object o)
    {
        throw new RuntimeException("should never get here");
    }

    public void setCharValue(Object o, char newValue)
    {
        throw new RuntimeException("should never get here");
    }
}

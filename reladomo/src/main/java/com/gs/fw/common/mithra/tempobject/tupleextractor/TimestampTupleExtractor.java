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

package com.gs.fw.common.mithra.tempobject.tupleextractor;

import com.gs.fw.common.mithra.extractor.AbstractTimestampExtractor;
import com.gs.fw.common.mithra.tempobject.Tuple;

import java.sql.Timestamp;


public class TimestampTupleExtractor extends AbstractTimestampExtractor<Tuple>
{
    private int pos;

    public TimestampTupleExtractor(int pos)
    {
        this.pos = pos;
    }

    public Timestamp timestampValueOf(Tuple o)
    {
        return o.getAttributeAsTimestamp(this.pos);
    }

    public void setTimestampValue(Tuple o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }
}
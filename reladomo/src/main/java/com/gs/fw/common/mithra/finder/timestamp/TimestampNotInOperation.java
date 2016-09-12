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

package com.gs.fw.common.mithra.finder.timestamp;

import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.finder.NonPrimitiveNotInOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Set;


public class TimestampNotInOperation extends NonPrimitiveNotInOperation
{

    public TimestampNotInOperation(NonPrimitiveAttribute attribute, Set set)
    {
        super(attribute, set);
    }

    public TimestampNotInOperation()
    {
        // for Externalizable
    }

    protected void writeParameter(ObjectOutput out, Object o) throws IOException
    {
        ((TimestampAttribute)this.getAttribute()).writeToStream(out, (Timestamp) o);
    }

    protected Object readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return ((TimestampAttribute)this.getAttribute()).readFromStream(in);
    }
}

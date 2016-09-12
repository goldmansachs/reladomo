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

package com.gs.fw.common.mithra.finder.time;

import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.attribute.TimeAttribute;
import com.gs.fw.common.mithra.finder.NonPrimitiveInOperation;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

public class TimeInOperation extends NonPrimitiveInOperation
{
    public TimeInOperation(NonPrimitiveAttribute attribute, Set set)
    {
        super(attribute, set);
    }

    public TimeInOperation()
    {
        // for Externalizable
    }

    protected void writeParameter(ObjectOutput out, Object o) throws IOException
    {
        ((TimeAttribute)this.getAttribute()).writeToStream(out, (Time) o);
    }

    protected Object readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return ((TimeAttribute)this.getAttribute()).readFromStream(in);
    }
}

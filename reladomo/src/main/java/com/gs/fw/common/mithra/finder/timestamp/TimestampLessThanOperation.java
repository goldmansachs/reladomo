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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.finder.NonPrimitiveLessThanOperation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;



public class TimestampLessThanOperation extends NonPrimitiveLessThanOperation implements Externalizable
{

    private transient long time;

    public TimestampLessThanOperation(Attribute attribute, Timestamp parameter)
    {
        super(attribute, parameter);
        this.time = parameter.getTime();
    }

    public TimestampLessThanOperation()
    {
        // for externalizable
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        Timestamp incoming = ((TimestampAttribute)this.getAttribute()).timestampValueOf(o);
        if (incoming == null) return false;
        return incoming.getTime() < this.time;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        TimestampAttribute attribute = (TimestampAttribute) in.readObject();
        this.setAttribute(attribute);
        final Timestamp parameter = attribute.readFromStream(in);
        this.time = parameter.getTime();
        this.setParameter(parameter);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        timestampAttribute.writeToStream(out, (Timestamp)this.getParameter());
    }

}

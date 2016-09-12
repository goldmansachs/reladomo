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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.sql.Timestamp;



public class ExternalizableDateRange implements Externalizable
{

    private AsOfAttribute businessAttribute;
    private Timestamp start;
    private Timestamp end;

    public ExternalizableDateRange(AsOfAttribute businessAttribute, Timestamp start, Timestamp end)
    {
        this.businessAttribute = businessAttribute;
        this.start = start;
        this.end = end;
    }

    public ExternalizableDateRange()
    {
    }

    public Timestamp getStart()
    {
        return start;
    }

    public Timestamp getEnd()
    {
        return end;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(businessAttribute);
        if (businessAttribute.getFromAttribute().requiresNoTimezoneConversion())
        {
            MithraTimestamp.writeTimezoneInsensitiveTimestampWithInfinity(out, start, businessAttribute.getInfinityDate());
            MithraTimestamp.writeTimezoneInsensitiveTimestampWithInfinity(out, end, businessAttribute.getInfinityDate());
        }
        else
        {
            MithraTimestamp.writeTimestampWithInfinity(out, start, businessAttribute.getInfinityDate());
            MithraTimestamp.writeTimestampWithInfinity(out, end, businessAttribute.getInfinityDate());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.businessAttribute = (AsOfAttribute) in.readObject();
        if (businessAttribute.getFromAttribute().requiresNoTimezoneConversion())
        {
            start = MithraTimestamp.readTimezoneInsensitiveTimestampWithInfinity(in, businessAttribute.getInfinityDate());
            end = MithraTimestamp.readTimezoneInsensitiveTimestampWithInfinity(in, businessAttribute.getInfinityDate());
        }
        else
        {
            start = MithraTimestamp.readTimestampWithInfinity(in, businessAttribute.getInfinityDate());
            end = MithraTimestamp.readTimestampWithInfinity(in, businessAttribute.getInfinityDate());
        }
    }
}

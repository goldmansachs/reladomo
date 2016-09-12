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

package com.gs.fw.common.mithra.finder.date;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.NonPrimitiveLessThanEqualsOperation;
import com.gs.fw.common.mithra.util.MithraTimestamp;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;



public class DateLessThanEqualsOperation extends NonPrimitiveLessThanEqualsOperation implements Externalizable
{

    public DateLessThanEqualsOperation(Attribute attribute, Date parameter)
    {
        super(attribute, parameter);
    }

    public DateLessThanEqualsOperation()
    {
        // for Externalizable
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        MithraTimestamp.writeTimezoneInsensitiveDate(out, (Date) this.getParameter());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setAttribute((Attribute) in.readObject());
        this.setParameter(MithraTimestamp.readTimezoneInsensitiveDate(in));
    }
}

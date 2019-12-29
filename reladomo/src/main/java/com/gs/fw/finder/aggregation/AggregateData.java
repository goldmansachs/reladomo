
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

package com.gs.fw.finder.aggregation;

import java.io.Externalizable;
import java.sql.Timestamp;
import java.util.Date;

public interface AggregateData extends Externalizable
{
    public Object[] getValues();

    public Object getValueAt(int position);

    public boolean getAttributeAsBoolean(String name);

    public byte getAttributeAsByte(String name);

    public byte[] getAttributeAsByteArray(String name);

    public char getAttributeAsCharacter(String name);

    public Date getAttributeAsDate(String name);

    public double getAttributeAsDouble(String name);

    public Enum getAttributeAsEnumeration(String name);

    public float getAttributeAsFloat(String name);

    public int getAttributeAsInteger(String name);

    public long getAttributeAsLong(String name);

    public short getAttributeAsShort(String name);

    public String getAttributeAsString(String name);

    public Timestamp getAttributeAsTimestamp(String name);
}

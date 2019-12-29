
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
    Object[] getValues();

    Object getValueAt(int position);

    boolean getAttributeAsBoolean(String name);

    byte getAttributeAsByte(String name);

    byte[] getAttributeAsByteArray(String name);

    char getAttributeAsCharacter(String name);

    Date getAttributeAsDate(String name);

    double getAttributeAsDouble(String name);

    Enum getAttributeAsEnumeration(String name);

    float getAttributeAsFloat(String name);

    int getAttributeAsInteger(String name);

    long getAttributeAsLong(String name);

    short getAttributeAsShort(String name);

    String getAttributeAsString(String name);

    Timestamp getAttributeAsTimestamp(String name);
}

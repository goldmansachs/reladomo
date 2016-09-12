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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.util.Time;

import java.util.Date;
import java.sql.Timestamp;
import java.io.OutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.math.BigDecimal;


public interface Tuple
{

    public Object getAttribute(int index);
    public boolean isAttributeNull(int index);

    public boolean getAttributeAsBoolean(int index);
    public byte getAttributeAsByte(int index);
    public short getAttributeAsShort(int index);
    public char getAttributeAsChar(int index);
    public int getAttributeAsInt(int index);
    public long getAttributeAsLong(int index);
    public float getAttributeAsFloat(int index);
    public double getAttributeAsDouble(int index);
    public String getAttributeAsString(int index);
    public Date getAttributeAsDate(int index);
    public Time getAttributeAsTime(int index);
    public Timestamp getAttributeAsTimestamp(int index);
    public byte[] getAttributeAsByteArray(int index);
    public BigDecimal getAttributeAsBigDecimal(int pos);
    public void writeToStream(ObjectOutput os) throws IOException;
}

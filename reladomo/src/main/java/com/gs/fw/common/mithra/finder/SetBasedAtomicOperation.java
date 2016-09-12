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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.PersisterId;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;



public interface SetBasedAtomicOperation
{

    public boolean getSetValueAsBoolean(int index);
    public byte getSetValueAsByte(int index);
    public short getSetValueAsShort(int index);
    public char getSetValueAsChar(int index);
    public int getSetValueAsInt(int index);
    public long getSetValueAsLong(int index);
    public float getSetValueAsFloat(int index);
    public double getSetValueAsDouble(int index);
    public String getSetValueAsString(int index);
    public Date getSetValueAsDate(int index);
    public Time getSetValueAsTime(int index);
    public Timestamp getSetValueAsTimestamp(int index);
    public byte[] getSetValueAsByteArray(int index);
    public BigDecimal getSetValueAsBigDecimal(int index);

    public int getSetSize();

    public boolean maySplit();

    public TupleTempContext createTempContextAndInsert(SqlQuery query);

    public String getSubSelectStringForTupleTempContext(TupleTempContext tempContext, Object source, PersisterId persisterId);

    public void generateTupleTempContextJoinSql(SqlQuery sqlQuery, TupleTempContext tempContext, Object source, PersisterId persisterId, int position, boolean inOrClause);
}

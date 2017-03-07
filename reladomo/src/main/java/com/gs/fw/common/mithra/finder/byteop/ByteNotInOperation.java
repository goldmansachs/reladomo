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

package com.gs.fw.common.mithra.finder.byteop;

import com.gs.collections.api.set.primitive.ByteSet;
import com.gs.collections.impl.factory.primitive.ByteSets;
import com.gs.fw.common.mithra.attribute.ByteAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.NotInOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;


public class ByteNotInOperation extends NotInOperation implements SqlParameterSetter
{
    private ByteSet set;
    private transient volatile byte[] copiedArray;

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public ByteNotInOperation(ByteAttribute attribute, ByteSet byteSet)
    {
        super(attribute);
        this.set = byteSet.freeze();
    }

    public ByteNotInOperation(ByteAttribute attribute, org.eclipse.collections.api.set.primitive.ByteSet byteSet)
    {
        super(attribute);
        this.set = ByteSets.immutable.of(byteSet.toArray());
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        ByteAttribute attribute = (ByteAttribute)this.getAttribute();
        if (attribute.isAttributeNull(o)) return false;
        return Boolean.valueOf(!this.set.contains(attribute.byteValueOf(o)));
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        populateCopiedArray();
        for(int i=setStart;i< setStart + numberToSet;i++)
        {
            pstmt.setByte(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    @Override
    public byte getSetValueAsByte(int index)
    {
        return this.copiedArray[index];
    }

    @Override
    protected void appendSetToString(ToStringContext toStringContext)
    {
        toStringContext.append(this.set.toString());
    }

    protected void populateCopiedArray()
    {
        if (this.copiedArray == null)
        {
            synchronized (this)
            {
                if (this.copiedArray == null) this.copiedArray = this.set.toArray();
            }
        }
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ this.set.hashCode());
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof ByteNotInOperation)
        {
            ByteNotInOperation other = (ByteNotInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public int getSetSize()
    {
        return this.set.size();
    }
}

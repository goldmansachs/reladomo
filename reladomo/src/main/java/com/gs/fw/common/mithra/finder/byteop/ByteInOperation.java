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
import com.gs.fw.common.mithra.extractor.ByteExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.finder.InOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.HashUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;


public class ByteInOperation extends InOperation implements SqlParameterSetter
{
    private ByteSet set;
    private transient volatile byte[] copiedArray;

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public ByteInOperation(ByteAttribute attribute, ByteSet byteSet)
    {
        super(attribute);
        this.set = byteSet.freeze();
    }

    public ByteInOperation(ByteAttribute attribute, org.eclipse.collections.api.set.primitive.ByteSet byteSet)
    {
        super(attribute);
        this.set = ByteSets.immutable.of(byteSet.toArray());
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        ByteAttribute attribute = (ByteAttribute)this.getAttribute();
        if (attribute.isAttributeNull(o)) return false;
        return Boolean.valueOf(this.set.contains(attribute.byteValueOf(o)));
    }

    @Override
    public List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this.set);
    }

    @Override
    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        for(int i=setStart;i<setStart+numberToSet;i++)
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

    @Override
    protected void populateCopiedArray()
    {
        if (this.copiedArray == null)
        {
            synchronized (this)
            {
                if (this.copiedArray == null)
                {
                    byte[] temp = this.set.toArray();
                    Arrays.sort(temp);
                    this.copiedArray = temp;
                }
            }
        }
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.set.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof ByteInOperation)
        {
            ByteInOperation other = (ByteInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    @Override
    public int getSetSize()
    {
        return this.set.size();
    }

    public Extractor getParameterExtractor()
    {
        populateCopiedArray();
        return new ParameterExtractor();
    }

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements ByteExtractor
    {
        public int intValueOf(Object o)
        {
            return (int) this.byteValueOf(o);
        }

        @Override
        public int getSetSize()
        {
            return ByteInOperation.this.getSetSize();
        }

        public byte byteValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.byteValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.byteValueOf(first) == this.byteValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((ByteExtractor) secondExtractor).byteValueOf(second) == this.byteValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return Byte.valueOf(this.byteValueOf(anObject));
        }
    }
}

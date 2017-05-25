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

package com.gs.fw.common.mithra.finder.longop;

import com.gs.collections.api.set.primitive.LongSet;
import com.gs.collections.impl.factory.primitive.LongSets;
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
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


public class LongInOperation extends InOperation implements SqlParameterSetter
{
    private LongSet set;
    private transient volatile long[] copiedArray;

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public LongInOperation(LongAttribute attribute, LongSet longSet)
    {
        super(attribute);
        this.set = longSet.freeze();
    }

    public LongInOperation(LongAttribute attribute, org.eclipse.collections.api.set.primitive.LongSet longSet)
    {
        super(attribute);
        this.set = LongSets.immutable.of(longSet.toArray());
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        LongAttribute attribute = (LongAttribute)this.getAttribute();
        if (attribute.isAttributeNull(o)) return false;
        return Boolean.valueOf(this.set.contains(attribute.longValueOf(o)));
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
            pstmt.setLong(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    @Override
    public long getSetValueAsLong(int index)
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
                    long[] temp = this.set.toArray();
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
        if (obj instanceof LongInOperation)
        {
            LongInOperation other = (LongInOperation) obj;
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

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements LongExtractor
    {
        @Override
        public int getSetSize()
        {
            return LongInOperation.this.getSetSize();
        }

        public long longValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.longValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.longValueOf(first) == this.longValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((LongExtractor) secondExtractor).longValueOf(second) == this.longValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return Long.valueOf(this.longValueOf(anObject));
        }
    }
}

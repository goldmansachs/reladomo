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

package com.gs.fw.common.mithra.finder.booleanop;

import com.gs.collections.api.set.primitive.BooleanSet;
import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;


public class BooleanInOperation extends InOperation implements SqlParameterSetter
{
    private BooleanSet set;
    private transient volatile boolean[] copiedArray;


    public BooleanInOperation(BooleanAttribute attribute, BooleanSet booleanSet)
    {
        super(attribute);
        this.set = booleanSet.freeze();
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        BooleanAttribute attribute = (BooleanAttribute)this.getAttribute();
        if (attribute.isAttributeNull(o)) return false;
        return Boolean.valueOf(this.set.contains(attribute.booleanValueOf(o)));
    }

    public List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this.set);
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        populateCopiedArray();
        for(int i=setStart;i< setStart + numberToSet;i++)
        {
            pstmt.setBoolean(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    @Override
    public boolean getSetValueAsBoolean(int index)
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
        return this.getAttribute().hashCode() ^ this.set.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof BooleanInOperation)
        {
            BooleanInOperation other = (BooleanInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public int getSetSize()
    {
        return this.set.size();
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return null;
    }

    public Extractor getParameterExtractor()
    {
        populateCopiedArray();
        return new ParameterExtractor();
    }

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements BooleanExtractor
    {
        public int getSetSize()
        {
            return BooleanInOperation.this.getSetSize();
        }

        public boolean booleanValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.booleanValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.booleanValueOf(first) == this.booleanValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((BooleanExtractor) secondExtractor).booleanValueOf(second) == this.booleanValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return Boolean.valueOf(this.booleanValueOf(anObject));
        }
    }
}

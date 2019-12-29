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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.finder.integer;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.finder.InOperation;
import com.gs.fw.common.mithra.finder.SourceOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.HashUtil;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;


public class IntegerInOperation extends InOperation implements SqlParameterSetter, SourceOperation
{
    private IntSet set;
    private transient volatile int[] copiedArray;


    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public IntegerInOperation(IntegerAttribute attribute, com.gs.collections.api.set.primitive.IntSet intSet)
    {
        super(attribute);
        this.set = IntSets.immutable.of(intSet.toArray());
    }

    public IntegerInOperation(IntegerAttribute attribute, IntSet intSet)
    {
        super(attribute);
        this.set = intSet.freeze();
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
            pstmt.setInt(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.set.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof IntegerInOperation)
        {
            IntegerInOperation other = (IntegerInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public Object getSourceAttributeValue(SqlQuery query, int sourceNumber, boolean isSelectedObject)
    {
        if (isSelectedObject || set.size() == 1)
        {
            populateCopiedArray();
            if (isSelectedObject) return Integer.valueOf(copiedArray[sourceNumber]);
            else return Integer.valueOf(copiedArray[0]);
        }
        throw new MithraBusinessException("cannot have multiple in operations for source keys");
    }

    public boolean isSameSourceOperation(SourceOperation other)
    {
        if (other instanceof IntegerInOperation)
        {
            IntegerInOperation iio = (IntegerInOperation) other;
            return this.set.equals(iio.set);
        }
        return false;
    }

    @Override
    public int getSetValueAsInt(int index)
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
                    int[] temp = this.set.toArray();
                    Arrays.sort(temp);
                    this.copiedArray = temp;
                }
            }
        }
    }

    public int getSourceAttributeValueCount()
    {
        return this.set.size();
    }

    @Override
    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        if (this.getAttribute().isSourceAttribute())
        {
            extractor.setSourceOperation(this);
        }
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

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements IntExtractor
    {
        @Override
        public int getSetSize()
        {
            return IntegerInOperation.this.getSetSize();
        }

        public int intValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.intValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.intValueOf(first) == this.intValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((IntExtractor) secondExtractor).intValueOf(second) == this.intValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return Integer.valueOf(this.intValueOf(anObject));
        }
    }

    @Override
    public boolean setContains(Object holder, Extractor extractor)
    {
        return this.set.contains(((IntExtractor)extractor).intValueOf(holder));
    }

    @Override
    protected ShapeMatchResult shapeMatchSet(InOperation existingOperation)
    {
        IntIterator integerIterator = this.set.intIterator();
        while(integerIterator.hasNext())
        {
            if (!((IntegerInOperation) existingOperation).set.contains(integerIterator.next()))
            {
                return NoMatchSmr.INSTANCE;
            }
        }
        return this.set.size() == existingOperation.getSetSize() ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
    }
}

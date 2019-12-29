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

package com.gs.fw.common.mithra.finder.shortop;

import com.gs.fw.common.mithra.attribute.ShortAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.ShortExtractor;
import com.gs.fw.common.mithra.finder.InOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.util.HashUtil;
import org.eclipse.collections.api.iterator.ShortIterator;
import org.eclipse.collections.api.set.primitive.ShortSet;
import org.eclipse.collections.impl.factory.primitive.ShortSets;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;


public class ShortInOperation extends InOperation implements SqlParameterSetter
{
    private ShortSet set;
    private transient volatile short[] copiedArray;

    public ShortInOperation(ShortAttribute attribute, ShortSet set)
    {
        super(attribute);
        this.set = set.freeze();
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
            pstmt.setShort(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    @Override
    public short getSetValueAsShort(int index)
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
                    short[] temp = this.set.toArray();
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
        if (obj instanceof ShortInOperation)
        {
            ShortInOperation other = (ShortInOperation) obj;
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

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements ShortExtractor
    {
        public int intValueOf(Object o)
        {
            return (int) this.shortValueOf(o);
        }

        @Override
        public int getSetSize()
        {
            return ShortInOperation.this.getSetSize();
        }

        public short shortValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.shortValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.shortValueOf(first) == this.shortValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((ShortExtractor) secondExtractor).shortValueOf(second) == this.shortValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return Short.valueOf(this.shortValueOf(anObject));
        }
    }

    @Override
    public boolean setContains(Object holder, Extractor extractor)
    {
        return this.set.contains(((ShortExtractor)extractor).shortValueOf(holder));
    }


    @Override
    protected ShapeMatchResult shapeMatchSet(InOperation existingOperation)
    {
        ShortIterator shortIterator = this.set.shortIterator();
        while(shortIterator.hasNext())
        {
            if (!((ShortInOperation) existingOperation).set.contains(shortIterator.next()))
            {
                return NoMatchSmr.INSTANCE;
            }
        }
        return this.set.size() == existingOperation.getSetSize() ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
    }
}

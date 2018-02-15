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

package com.gs.fw.common.mithra.finder.floatop;

import com.gs.collections.api.iterator.FloatIterator;
import com.gs.collections.api.set.primitive.FloatSet;
import com.gs.collections.impl.factory.primitive.FloatSets;
import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.FloatExtractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.finder.InOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.util.HashUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;


public class FloatInOperation extends InOperation implements SqlParameterSetter
{
    private FloatSet set;
    private transient volatile float[] copiedArray;


    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public FloatInOperation(FloatAttribute attribute, FloatSet floatSet)
    {
        super(attribute);
        this.set = floatSet.freeze();
    }

    public FloatInOperation(FloatAttribute attribute, org.eclipse.collections.api.set.primitive.FloatSet floatSet)
    {
        super(attribute);
        this.set = FloatSets.immutable.of(floatSet.toArray());
    }

    public List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this.set);
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        for(int i=setStart;i<setStart+numberToSet;i++)
        {
            pstmt.setFloat(startIndex++, copiedArray[i]);
        }
        return numberToSet;
    }

    @Override
    public float getSetValueAsFloat(int index)
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
                if (this.copiedArray == null)
                {
                    float[] temp = this.set.toArray();
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
        if (obj instanceof FloatInOperation)
        {
            FloatInOperation other = (FloatInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public int getSetSize()
    {
        return this.set.size();
    }

    public Extractor getParameterExtractor()
    {
        populateCopiedArray();
        return new ParameterExtractor();
    }

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements FloatExtractor
    {
        public int getSetSize()
        {
            return FloatInOperation.this.getSetSize();
        }

        public float floatValueOf(Object o)
        {
            return copiedArray[this.getPosition()];
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.floatValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.floatValueOf(first) == this.floatValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((FloatExtractor) secondExtractor).floatValueOf(second) == this.floatValueOf(first);
        }

        public Object valueOf(Object anObject)
        {
            return new Float(this.floatValueOf(anObject));
        }
    }

    @Override
    public boolean setContains(Object holder, Extractor extractor)
    {
        return this.set.contains(((FloatExtractor)extractor).floatValueOf(holder));
    }

    @Override
    protected ShapeMatchResult shapeMatchSet(InOperation existingOperation)
    {
        FloatIterator floatIterator = this.set.floatIterator();
        while(floatIterator.hasNext())
        {
            if (!((FloatInOperation) existingOperation).set.contains(floatIterator.next()))
            {
                return NoMatchSmr.INSTANCE;
            }
        }
        return this.set.size() == existingOperation.getSetSize() ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
    }
}

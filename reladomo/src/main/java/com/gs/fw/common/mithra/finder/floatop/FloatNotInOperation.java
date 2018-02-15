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
import com.gs.fw.common.mithra.finder.NotInOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.TimeZone;


public class FloatNotInOperation extends NotInOperation implements SqlParameterSetter
{
    private FloatSet set;
    private transient volatile float[] copiedArray;


    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public FloatNotInOperation(FloatAttribute attribute, FloatSet floatSet)
    {
        super(attribute);
        this.set = floatSet.freeze();
    }

    public FloatNotInOperation(FloatAttribute attribute, org.eclipse.collections.api.set.primitive.FloatSet floatSet)
    {
        super(attribute);
        this.set = FloatSets.immutable.of(floatSet.toArray());
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        populateCopiedArray();
        for(int i=setStart;i< setStart + numberToSet;i++)
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
                    this.copiedArray = this.set.toArray();
                    Arrays.sort(this.copiedArray);
                }
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
        if (obj instanceof FloatNotInOperation)
        {
            FloatNotInOperation other = (FloatNotInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public int getSetSize()
    {
        return this.set.size();
    }

    @Override
    public boolean setContains(Object holder, Extractor extractor)
    {
        return this.set.contains(((FloatExtractor)extractor).floatValueOf(holder));
    }

    @Override
    protected ShapeMatchResult shapeMatchSet(NotInOperation existingOperation)
    {
        if (existingOperation.getSetSize() < MAX_SHAPE_MATCH_SIZE)
        {
            FloatNotInOperation loopOp = (FloatNotInOperation) existingOperation;
            FloatIterator floatIterator = loopOp.set.floatIterator();
            while(floatIterator.hasNext())
            {
                if (!this.set.contains(floatIterator.next()))
                {
                    return NoMatchSmr.INSTANCE;
                }
            }
            return (this.getSetSize() == loopOp.getSetSize()) ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
        }
        return NoMatchSmr.INSTANCE;
    }
}

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

package com.gs.fw.common.mithra.finder.charop;

import com.gs.fw.common.mithra.attribute.CharAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.CharExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.NotInOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import org.eclipse.collections.api.iterator.CharIterator;
import org.eclipse.collections.api.set.primitive.CharSet;
import org.eclipse.collections.impl.factory.primitive.CharSets;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.TimeZone;


public class CharNotInOperation extends NotInOperation implements SqlParameterSetter
{
    private CharSet set;
    private transient volatile char[] copiedArray;


    public CharNotInOperation(CharAttribute attribute, CharSet charSet)
    {
        super(attribute);
        this.set = charSet.freeze();
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        populateCopiedArray();
        for(int i=setStart;i< setStart + numberToSet;i++)
        {
            char value = this.copiedArray[i];
            pstmt.setString(startIndex++, new String(new char[] { value }));
        }
        return numberToSet;
    }

    @Override
    public char getSetValueAsChar(int index)
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
        if (obj instanceof CharNotInOperation)
        {
            CharNotInOperation other = (CharNotInOperation) obj;
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
        return this.set.contains(((CharExtractor)extractor).charValueOf(holder));
    }

    @Override
    protected ShapeMatchResult shapeMatchSet(NotInOperation existingOperation)
    {
        if (existingOperation.getSetSize() < MAX_SHAPE_MATCH_SIZE)
        {
            CharNotInOperation loopOp = (CharNotInOperation) existingOperation;
            CharIterator charIterator = loopOp.set.charIterator();
            while(charIterator.hasNext())
            {
                if (!this.set.contains(charIterator.next()))
                {
                    return NoMatchSmr.INSTANCE;
                }
            }
            return (this.getSetSize() == loopOp.getSetSize()) ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
        }
        return NoMatchSmr.INSTANCE;
    }
}

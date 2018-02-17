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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.util.Time;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;



public class NonPrimitiveNotInOperation extends NotInOperation implements SqlParameterSetter, Externalizable
{
    private Set set;
    private transient volatile Object[] copiedArray;

    public NonPrimitiveNotInOperation(NonPrimitiveAttribute attribute, Set set)
    {
        super(attribute);
        this.set = set;
    }

    public NonPrimitiveNotInOperation()
    {
        // for Externalizable
    }

    @Override
    protected int getMaxLength()
    {
        if (this.getAttribute() instanceof StringAttribute)
        {
            Set<String> strings = (Set<String>) set;
            int max = 0;
            for(Iterator<String> it = strings.iterator(); it.hasNext();)
            {
                String next = it.next();
                if (next != null && next.length() > max)
                {
                    max = next.length();
                }
            }
            return max;
        }
        return 0;
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
        populateCopiedArray();
        for(int i=setStart;i<setStart+numberToSet;i++)
        {
            ((NonPrimitiveAttribute)this.getAttribute()).setSqlParameter(startIndex++, pstmt, copiedArray[i], timeZone, databaseType);
        }
        return numberToSet;
    }

    protected void populateCopiedArray()
    {
        if (this.copiedArray == null)
        {
            synchronized (this)
            {
                if (copiedArray == null)
                {
                    copiedArray = new Object[set.size()];
                    set.toArray(copiedArray);
                    if (copiedArray.length > 1 && !set.contains(null) && copiedArray[0] instanceof Comparable)
                    {
                        Arrays.sort(this.copiedArray);
                    }
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
        if (obj instanceof NonPrimitiveNotInOperation)
        {
            NonPrimitiveNotInOperation other = (NonPrimitiveNotInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public int getSetSize()
    {
        return this.set.size();
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        int size = this.getSetSize();
        out.writeInt(size);
        out.writeBoolean(set instanceof ByteArraySet);
        for (Iterator it = this.set.iterator(); size > 0; size--)
        {
            this.writeParameter(out, it.next());
        }
    }

    protected void writeParameter(ObjectOutput out, Object o) throws IOException
    {
        out.writeObject(o);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setAttribute((Attribute) in.readObject());
        int size = in.readInt();
        boolean isByteArray = in.readBoolean();
        if (isByteArray)
        {
            this.set = new ByteArraySet(size);
        }
        else
        {
            this.set = new UnifiedSet(size);
        }
        for(int i=0;i<size;i++)
        {
            set.add(this.readParameter(in));
        }
    }

    protected Object readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return in.readObject();
    }

    @Override
    public byte[] getSetValueAsByteArray(int index)
    {
        return (byte[]) copiedArray[index];
    }

    @Override
    public Date getSetValueAsDate(int index)
    {
        return (Date) copiedArray[index];
    }

    @Override
    public Time getSetValueAsTime(int index)
    {
        return (Time) copiedArray[index];
    }

    @Override
    public String getSetValueAsString(int index)
    {
        return (String) copiedArray[index];
    }

    @Override
    public Timestamp getSetValueAsTimestamp(int index)
    {
        return (Timestamp) copiedArray[index];
    }

    @Override
    protected void appendSetToString(ToStringContext toStringContext)
    {
        toStringContext.append(this.set.toString());
    }

    @Override
    public BigDecimal getSetValueAsBigDecimal(int index)
    {
        return (BigDecimal) copiedArray[index];
    }


    @Override
    public boolean setContains(Object holder, Extractor extractor)
    {
        return this.set.contains(extractor.valueOf(holder));
    }

    @Override
    protected ShapeMatchResult shapeMatchSet(NotInOperation existingOperation)
    {
        if (existingOperation.getSetSize() < MAX_SHAPE_MATCH_SIZE)
        {
            NonPrimitiveNotInOperation loopOp = (NonPrimitiveNotInOperation) existingOperation;
            Iterator floatIterator = loopOp.set.iterator();
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

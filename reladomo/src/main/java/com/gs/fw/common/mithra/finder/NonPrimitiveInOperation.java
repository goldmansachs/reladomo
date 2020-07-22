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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.StringPool;
import com.gs.fw.common.mithra.util.Time;
import org.eclipse.collections.impl.set.mutable.SetAdapter;
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
import java.util.List;
import java.util.Set;
import java.util.TimeZone;



public class NonPrimitiveInOperation extends InOperation implements SqlParameterSetter, SourceOperation, Externalizable
{
    private static final long serialVersionUID = -182673430688996501L;

    private Set set;
    protected transient volatile Object[] copiedArray;

    public NonPrimitiveInOperation(NonPrimitiveAttribute attribute, Set set)
    {
        super(attribute);
        this.set = set;
    }

    public NonPrimitiveInOperation()
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

    protected Set getSet()
    {
        return set;
    }

    public List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this.set);
    }

    protected int setSqlParameters(PreparedStatement pstmt, int startIndex, TimeZone timeZone, int setStart, int numberToSet, DatabaseType databaseType) throws SQLException
    {
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
                    Object[] temp = new Object[set.size()];
                    set.toArray(temp);
                    if (temp.length > 1 && !set.contains(null) && temp[0] instanceof Comparable)
                    {
                        Arrays.sort(temp);
                    }
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
        if (obj instanceof NonPrimitiveInOperation)
        {
            NonPrimitiveInOperation other = (NonPrimitiveInOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.set.equals(other.set);
        }
        return false;
    }

    public Object getSourceAttributeValue(SqlQuery query, int sourceNumber, boolean isSelectedObject)
    {
        if (isSelectedObject || set.size() == 1)
        {
            this.populateCopiedArray();
            if (isSelectedObject) return copiedArray[sourceNumber];
            else return copiedArray[0];
        }
        throw new MithraBusinessException("'in' operations for source keys on related objects are not yet implemented!");
    }

    public boolean isSameSourceOperation(SourceOperation other)
    {
        if (other instanceof NonPrimitiveInOperation)
        {
            NonPrimitiveInOperation npio = (NonPrimitiveInOperation) other;
            return this.set.equals(npio.set);
        }
        return false;
    }

    public int getSourceAttributeValueCount()
    {
        return this.set.size();
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        if(this.getAttribute().isSourceAttribute())
        {
            extractor.setSourceOperation(this);
        }
    }

    public int getSetSize()
    {
        return this.set.size();
    }

    public Extractor getParameterExtractor()
    {
        populateCopiedArray();
        return createParameterExtractor();
    }

    protected Extractor createParameterExtractor()
    {
        return new ParameterExtractor();
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeExternalForSublcass(out);
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
        this.readExternalForSublcass(in);
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

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements StringExtractor
    {
        public int getSetSize()
        {
            return NonPrimitiveInOperation.this.getSetSize();
        }

        public boolean isAttributeNull(Object o)
        {
            return this.valueOf(o) == null;
        }

        public int valueHashCode(Object o)
        {
            return valueOf(o).hashCode();
        }

        public boolean valueEquals(Object first, Object second)
        {
            if (first == second) return true;
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = this.isAttributeNull(second);
            if (firstNull) return secondNull;
            return this.valueOf(first).equals(this.valueOf(second));
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            Object firstValue = this.valueOf(first);
            Object secondValue = secondExtractor.valueOf(second);
            if (firstValue == secondValue) return true; // takes care of both null

            return (firstValue != null) && firstValue.equals(secondValue);
        }

        public Object valueOf(Object anObject)
        {
            return copiedArray[this.getPosition()];
        }

        @Override
        public int offHeapValueOf(Object o)
        {
            return StringPool.getInstance().getOffHeapAddressWithoutAdding((String) copiedArray[this.getPosition()]);
        }

        @Override
        public String stringValueOf(Object o)
        {
            return (String) copiedArray[this.getPosition()];
        }

        @Override
        public void setStringValue(Object o, String newValue)
        {
            throw new RuntimeException("not implemented");
        }
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
        NonPrimitiveAttribute nonPrimitiveAttribute = (NonPrimitiveAttribute) this.getAttribute();
        String formattedStrings = SetAdapter.adapt(this.set)
                .asLazy()
                .collect(nonPrimitiveAttribute::formattedValue)
                .toString();
        toStringContext.append(formattedStrings);
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
    protected ShapeMatchResult shapeMatchSet(InOperation existingOperation)
    {
        Iterator floatIterator = this.set.iterator();
        while(floatIterator.hasNext())
        {
            if (!((NonPrimitiveInOperation) existingOperation).set.contains(floatIterator.next()))
            {
                return NoMatchSmr.INSTANCE;
            }
        }
        return this.set.size() == existingOperation.getSetSize() ? ExactMatchSmr.INSTANCE : new SuperMatchSmr(existingOperation, this);
    }
}


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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.AggregateAttribute;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.ByteArrayUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.ByteArrayExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArrayEqOperation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.finder.orderby.ByteArrayOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public abstract class ByteArrayAttribute<Owner> extends NonPrimitiveAttribute<Owner, byte[]> implements com.gs.fw.finder.attribute.ByteArrayAttribute<Owner>, ByteArrayExtractor<Owner, byte[]>
{
    private int maxLength = Integer.MAX_VALUE;
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = -2871034560645486938L;

    public ByteArrayAttribute(int maxLength)
    {
        this.maxLength = maxLength;
    }

    public ByteArrayAttribute()
    {
    }

    @Override
    protected void serializedNonNullValue(Owner o, ObjectOutput out) throws IOException
    {
        byte[] bytes = this.byteArrayValueOf(o);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    protected void deserializedNonNullValue(Owner o, ObjectInput in) throws IOException
    {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        this.setByteArrayValue(o, bytes);
    }

    public int getMaxLength()
    {
        return maxLength;
    }

    protected void setMaxLength(int maxLength)
    {
        this.maxLength = maxLength;
    }

    @Override
    public int valueHashCode(Owner o)
    {
        byte[] bytes = this.byteArrayValueOf(o);
        if (bytes == null)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(bytes);
    }

    @Override
    public boolean valueEquals(Owner first, Owner second)
    {
        if (first == second)
        {
            return true;
        }
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull)
        {
            return secondNull;
        }
        return ByteArrayEqOperation.byteArrayEquals(this.valueOf(first), this.valueOf(second));
    }

    @Override
    public <O> boolean valueEquals(Owner first, O second, Extractor<O, byte[]> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return ByteArrayEqOperation.byteArrayEquals(this.valueOf(first), secondExtractor.valueOf(second));
        }
        return true;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final ByteArraySet set = new ByteArraySet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = extractor.valueOf(objects.get(i));
            if (o != null)
            {
                set.add((byte[]) o);
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final ByteArraySet set = new ByteArraySet();
        for (Object object : objects)
        {
            final Object o = extractor.valueOf(object);
            if (o != null)
            {
                set.add((byte[]) o);
            }
        }
        return this.in(set);
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((byte[]) other);
    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new ByteArrayOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new ByteArrayOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public abstract Operation eq(byte[] other);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(ByteArrayAttribute other);

    public abstract Operation joinEq(ByteArrayAttribute other);

    public abstract Operation filterEq(ByteArrayAttribute other);

    @Override
    public Operation notEq(byte[] other)
    {
        throw new UnsupportedOperationException("notEq is not supported for byte array attributes");
    }

    @Override
    public Operation in(Set<byte[]> set)
    {
        return this.in((ByteArraySet) set);
    }

    @Override
    public Operation notIn(Set<byte[]> set)
    {
        return this.notIn((ByteArraySet) set);
    }

    public abstract Operation in(ByteArraySet set);

    public abstract Operation notIn(ByteArraySet set);

    @Override
    protected Set newSetForInClause()
    {
        return new ByteArraySet();
    }

    @Override
    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setBytes(index, (byte[]) o);
    }

    public byte[] valueOf(Owner o)
    {
        return this.byteArrayValueOf(o);
    }

    public abstract byte[] byteArrayValueOf(Owner o);

    public void setValue(Owner o, byte[] newValue)
    {
        this.setByteArrayValue(o, newValue);
    }

    public abstract void setByteArrayValue(Owner o, byte[] bytes);

    @Override
    public Class valueType()
    {
        return byte[].class;
    }

    @Override
    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        this.parseWordAndSet(value, data, lineNumber);
    }

    @Override
    public void parseWordAndSet(String word, Owner data, int lineNumber) throws ParseException
    {
        if (word.equals("null"))
        {
            this.setValueNull(data);
        }
        else
        {
            if (word.length() % 2 > 0)
            {
                throw new ParseException(
                        "Could not parse " + word + " on line " + lineNumber
                                + " because it has to have an even number of hex digits", 0
                );
            }

            byte[] parsedBytes = new byte[word.length() / 2];
            for (int i = 0; i < word.length(); i += 2)
            {
                parsedBytes[i / 2] = parseByte(word.charAt(i), word.charAt(i + 1), lineNumber);
            }

            this.setByteArrayValue(data, parsedBytes);
        }
    }

    private byte parseByte(char first, char second, int lineNumber) throws ParseException
    {
        int result = getIntFromChar(first, lineNumber) * 16 + getIntFromChar(second, lineNumber);
        if (result > 127)
        {
            result -= -256;
        }
        return (byte) (result & 0xFF);
    }

    private int getIntFromChar(char first, int lineNumber) throws ParseException
    {
        if (first >= '0' && first <= '9')
        {
            return first - '0';
        }
        if (first >= 'A' && first <= 'F')
        {
            return first - 'A' + 10;
        }
        throw new ParseException("Could not parse '" + first + "' character on line " + lineNumber, 0);
    }

    @Override
    public void setValueUntil(Owner o, byte[] newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Object o, byte[] bytes, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        byte[] bytes = this.byteArrayValueOf(object);

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < bytes.length; i++)
        {
            buffer.append(formatter.format(bytes[i]));
        }

        return buffer.toString();
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((Owner) dataObjects[0]))
        {
            return 1;
        }
        byte[] firstValue = this.valueOf((Owner) dataObjects[0]);
        ByteArraySet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            byte[] nextValue = this.valueOf((Owner) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (!ByteArrayEqOperation.byteArrayEquals(nextValue, firstValue))
            {
                set = new ByteArraySet();
                set.add(firstValue);
                set.add(nextValue);
            }
        }
        if (set != null)
        {
            return set.size();
        }
        return 1;
    }

    @Override
    public AggregateAttribute min()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public AggregateAttribute max()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new ByteArrayUpdateWrapper(this, data, null);
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(new byte[0]);
    }
}

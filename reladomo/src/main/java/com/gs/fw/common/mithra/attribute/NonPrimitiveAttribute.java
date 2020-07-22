
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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.NonPrimitiveInOperation;
import com.gs.fw.common.mithra.finder.NonPrimitiveNotInOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.NonPrimitiveOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MutableComparableReference;
import com.gs.fw.common.mithra.util.Nullable;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public abstract class NonPrimitiveAttribute<Owner, Type> extends Attribute<Owner, Type>
{

    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = 7905290145061164927L;

    public NonPrimitiveAttribute()
    {
    }

    public boolean isAttributeNull(Owner o)
    {
        return this.valueOf(o) == null;
    }

    protected void serializedNonNullValue(Owner o, ObjectOutput out) throws IOException
    {
        out.writeObject(this.valueOf(o));
    }

    protected void deserializedNonNullValue(Owner o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setValue(o, (Type) in.readObject());
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeObject(valueWrappedInNullable.getAsObject());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableComparableReference((Comparable) in.readObject());
    }

    @Override
    public void copyValueFrom(Owner dest, Owner src)
    {
        this.setValue(dest, this.valueOf(src));
    }

    public int valueHashCode(Owner o)
    {
        Object val = this.valueOf(o);
        if (val == null) return HashUtil.NULL_HASH;
        return val.hashCode();
    }

    public boolean valueEquals(Owner first, Owner second)
    {
        if (first == second) return true;
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull) return secondNull;
        return this.valueOf(first).equals(this.valueOf(second));
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, Type> secondExtractor)
    {
        Object firstValue = this.valueOf(first);
        Object secondValue = secondExtractor.valueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new NonPrimitiveOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new NonPrimitiveOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    public void setValueNull(Owner o)
    {
        this.setValue(o, null);
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final UnifiedSet set = new UnifiedSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = extractor.valueOf(objects.get(i));
            if (o != null)
            {
                set.add(o);
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final UnifiedSet set = new UnifiedSet();
        for (Object object : objects)
        {
            final Object o = extractor.valueOf(object);
            if (o != null)
            {
                set.add(o);
            }
        }
        return this.in(set);
    }

    protected Set newSetForInClause()
    {
        return new UnifiedSet();
    }

    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        Set set = newSetForInClause();
        for(int i=0;i<objects.size();i++)
        {
            Object o = extractor.valueOf(objects.get(i));
            if (o != null)
            {
                set.add(o);
                if (set.size() > maxInClause)
                {
                    return new None(this);
                }
            }
        }
        return this.in(set);
    }

    public void setValueUntil(Owner o, Type newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public void setValueNullUntil(Owner o, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((Owner) dataObjects[0]))
        {
            return 1;
        }
        Object firstValue = this.valueOf((Owner) dataObjects[0]);
        UnifiedSet set = null;
        for(int i=1;i<dataObjects.length;i++)
        {
            Object nextValue = this.valueOf((Owner) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (!nextValue.equals(firstValue))
            {
                set = new UnifiedSet();
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

    public Operation zGetOperationFromResult(Owner result, Map<Attribute, Object> tempOperationPool)
    {
        return createOrGetOperation(tempOperationPool, this, this.valueOf(result));
    }

    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map<Attribute, Object> tempOperationPool)
    {
        return createOrGetOperation(tempOperationPool, this, left.valueOf(original));
    }

    private Operation createOrGetOperation(Map<Attribute, Object> tempOperationPool, NonPrimitiveAttribute right, Object value)
    {
        Map<Object, Operation> values = (Map<Object, Operation>) tempOperationPool.get(right);
        Operation existing = null;
        if (values == null)
        {
            values = new UnifiedMap();
            tempOperationPool.put(right, values);
        }
        else
        {
            existing = values.get(value);
        }
        if (existing == null)
        {
            existing = right.eq(value);
            if (values.size() < 100) values.put(value, existing);
        }
        return existing;
    }

    protected Operation getNotInOperation(NonPrimitiveAttribute attribute, Set<Type> set)
    {
        return new NonPrimitiveNotInOperation(attribute, set);
    }

    protected Operation getInOperation(NonPrimitiveAttribute attribute, Set<Type> set)
    {
        return new NonPrimitiveInOperation(attribute, set);
    }

    public Operation in(Set<Type> set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.iterator().next());
                break;
            default:
                op = getInOperation(this, set);
                break;
        }
        return op;
    }

    public Operation notIn(Set<Type> set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.iterator().next());
                break;
            default:
                op = getNotInOperation(this, set);
                break;
        }
        return op;
    }


    public abstract Operation eq(Type other);

    public abstract Operation notEq(Type other);

//    public abstract Operation in(Set<Type> set);
//
//    public abstract Operation notIn(Set<Type> set);

    public abstract void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException;

    public String formattedValue(Type object)
    {
        if (object == null)
        {
            return "null";
        }
        return "\"" + object + "\"";
    }
}

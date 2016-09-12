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

package com.gs.fw.common.mithra;

import com.gs.collections.api.block.procedure.primitive.ObjectIntProcedure;
import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.util.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 *
 * Version     : : 1.17 $
 *
 *
 */

public class AggregateDataConfig implements Serializable
{
    private ObjectIntHashMap<String> nameToPositionMap;
    private List<MithraGroupByAttribute> groupByAttributes;
    private List<MithraAggregateAttribute> aggregateAttributes;
    private static final byte NULL_VALUE = 100;
    private static final byte NOT_NULL_VALUE = 50;

    public AggregateDataConfig()
    {
    }

    public AggregateDataConfig(Map<String, MithraGroupByAttribute> groupAttributesByName, Map<String, MithraAggregateAttribute> aggAggtributesByName)
    {
        groupByAttributes = FastList.newList(groupAttributesByName.size());
        aggregateAttributes = FastList.newList(aggAggtributesByName.size());
        nameToPositionMap = new ObjectIntHashMap();
        int position = 1;
        Set<String> groupByNames = groupAttributesByName.keySet();
        for (String name : groupByNames)
        {
            nameToPositionMap.put(name, position++);
            groupByAttributes.add(groupAttributesByName.get(name));
        }

        Set<String> aggregateAttributeNames = aggAggtributesByName.keySet();
        for (String name : aggregateAttributeNames)
        {
            nameToPositionMap.put(name, position++);
            aggregateAttributes.add(aggAggtributesByName.get(name));
        }

        groupByAttributes = Collections.unmodifiableList(groupByAttributes);
        aggregateAttributes = Collections.unmodifiableList(aggregateAttributes);
    }

    public int getAttributeCount()
    {
        return nameToPositionMap.size();
    }

    public void setNameToPositionMap(ObjectIntHashMap<String> map)
    {
        this.nameToPositionMap = new ObjectIntHashMap(map.size());
        map.forEachKeyValue(new ObjectIntProcedure<String>()
        {
            @Override
            public void value(String each, int parameter)
            {
                nameToPositionMap.put(each, parameter);
            }
        });
    }

    public int getIndexForName(String name)
    {
        return this.nameToPositionMap.get(name);
    }

    public void writeToStream(ObjectOutput out, AggregateData aggData) throws IOException
    {
        int groupByListSize = groupByAttributes.size();
        for (int i = 0; i < groupByListSize; i++)
        {
            Attribute attribute = this.groupByAttributes.get(i).getAttribute();
            writeToStream(out, (Nullable) aggData.getValueAt(i), attribute, null);
        }
        for (int i = 0; i < aggregateAttributes.size(); i++)
        {
            AggregateAttributeCalculator calculator = aggregateAttributes.get(i).getCalculator();
            writeToStream(out, (Nullable) aggData.getValueAt(i + groupByListSize), null, calculator);
        }
    }

    private static void writeToStream(ObjectOutput out, Nullable nullable, Attribute attribute, AggregateAttributeCalculator calculator)
            throws IOException
    {
        Nullable valueWrappedInNullable = nullable;
        if (valueWrappedInNullable.isNull())
        {
            out.writeByte(NULL_VALUE);
        }
        else
        {
            out.writeByte(NOT_NULL_VALUE);
            if (attribute != null)
            {
                attribute.serializeNonNullAggregateDataValue(valueWrappedInNullable, out);
            }
            else
            {
                calculator.serializeNonNullAggregateDataValue(valueWrappedInNullable, out);
            }
        }
    }

    public void readFromStream(ObjectInput in, AggregateData aggregateData) throws IOException, ClassNotFoundException
    {
        int groupByListSize = groupByAttributes.size();
        for (int i = 0; i < groupByListSize; i++)
        {
            MithraGroupByAttribute groupByAttribute = groupByAttributes.get(i);

            Nullable valueWrappedInNullable = readFromStream(in, null, groupByAttribute);
            aggregateData.setValueAt(i, valueWrappedInNullable);
        }

        for (int i = 0; i < aggregateAttributes.size(); i++)
        {
            MithraAggregateAttribute aggregateAttribute = aggregateAttributes.get(i);

            Nullable valueWrappedInNullable = readFromStream(in, aggregateAttribute, null);
            aggregateData.setValueAt(i + groupByListSize, valueWrappedInNullable);
        }
    }

    private static Nullable readFromStream(ObjectInput in, MithraAggregateAttribute aggregateAttribute, MithraGroupByAttribute groupByAttribute)
            throws IOException, ClassNotFoundException
    {
        byte result = in.readByte();
        Nullable valueWrappedInNullable;
        switch (result)
        {
            case NULL_VALUE:
                valueWrappedInNullable = groupByAttribute != null ? groupByAttribute.getNullGroupByAttribute() : aggregateAttribute.getDefaultValueForEmptyGroup();
                break;
            case NOT_NULL_VALUE:
                valueWrappedInNullable = groupByAttribute != null ? groupByAttribute.getAttribute().deserializeNonNullAggregateDataValue(in) : aggregateAttribute.getCalculator().deserializeNonNullAggregateDataValue(in);
                break;
            default:
                throw new IOException("unexpected byte in stream " + result);
        }
        return valueWrappedInNullable;
    }

    public List<MithraGroupByAttribute> getGroupByAttributes()
    {
        return groupByAttributes;
    }

    public List<MithraAggregateAttribute> getAggregateAttributes()
    {
        return aggregateAttributes;
    }

    public boolean hasGroupBy()
    {
        return !groupByAttributes.isEmpty();
    }

    private int getIndexFor(String s)
    {
        int index = this.getIndexForName(s) - 1;
        if (index < 0)
        {
            throw new RuntimeException("Invalid attribute " + s);
        }
        return index;
    }

    public Object getObjectValueForName(String name, Object values)
    {
        Object[] val = (Object[]) values;
        return val[this.getIndexFor(name)];
    }

    public Object[] getValuesAsArray(Object values)
    {
        return (Object[]) values;
    }

    public void setValueAt(int position, Object value, Object values)
    {
        ((Object[]) values)[position] = value;
    }
}
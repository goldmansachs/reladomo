
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

package com.gs.fw.common.mithra.finder.orderby;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.finder.AsOfEqualityChecker;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.Set;

public abstract class AttributeBasedOrderBy implements OrderBy
{

    private Attribute attribute;
    private boolean isAscending;

    protected abstract int compareAscending(Object left, Object right);

    public AttributeBasedOrderBy(Attribute attribute, boolean ascending)
    {
        this.attribute = attribute;
        isAscending = ascending;
    }

    public int compare(Object left, Object right)
    {
        boolean leftNull = this.attribute.isAttributeNull(left);
        boolean rightNull = this.attribute.isAttributeNull(right);
        int result = 0;
        if (leftNull)
        {
            if (rightNull)
            {
                result = 0;
            }
            else
            {
                result = -1;
            }
        }
        else if (rightNull)
        {
            result = 1;
        }
        if (!(leftNull || rightNull)) result = this.compareAscending(left, right);
        if (!this.isAscending) result = -result;
        return result;
    }

    public void generateSql(SqlQuery query)
    {
        String columnName = this.attribute.getFullyQualifiedLeftHandExpression(query);
        Mapper mapper = null;
        if (this.attribute instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) this.attribute;
            mapper = mappedAttribute.getMapper();
            mapper.pushMappers(query);
        }
        query.addOrderBy(columnName, this.isAscending);
        if (mapper != null) mapper.popMappers(query);
    }

    public Attribute getAttribute()
    {
        return attribute;
    }

    public OrderBy and(com.gs.fw.finder.OrderBy other)
    {
        return new ChainedOrderBy(this, (com.gs.fw.common.mithra.finder.orderby.OrderBy) other);
    }

    public void generateMapperSql(SqlQuery sqlQuery)
    {
        if (this.attribute instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) this.attribute;
            Mapper mapper = mappedAttribute.getMapper();
            mapper.generateSql(sqlQuery);
            mapper.popMappers(sqlQuery);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        Attribute a = this.attribute;
        while (a instanceof MappedAttribute)
        {
            a = ((MappedAttribute) a).getWrappedAttribute();
        }
        set.add(a);
    }

    public void registerSourceOperation(MithraDatabaseIdentifierExtractor extractor)
    {
        if (this.attribute instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) this.attribute;
            Mapper mapper = mappedAttribute.getMapper();
            mapper.registerOperation(extractor, true);
            mapper.popMappers(extractor);
        }
    }

    public void registerAsOfAttributes(AsOfEqualityChecker checker)
    {
        if (this.attribute instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) this.attribute;
            Mapper mapper = mappedAttribute.getMapper();
            mapper.registerAsOfAttributesAndOperations(checker);
            mapper.popMappers(checker);
        }
    }

    public boolean mustUseServerSideOrderBy()
    {
        return this.attribute instanceof MappedAttribute;
    }

    public int hashCode()
    {
        return attribute.hashCode() ^ HashUtil.hash(isAscending);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof AttributeBasedOrderBy)
        {
            AttributeBasedOrderBy attributeBasedOrderBy = ((AttributeBasedOrderBy) obj);
            return attributeBasedOrderBy.attribute.equals(this.attribute) && attributeBasedOrderBy.isAscending == this.isAscending;
        }
        return false;
    }
}

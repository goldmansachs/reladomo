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

import com.gs.collections.api.set.primitive.MutableIntSet;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.aggregate.AggregateOrderBy;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.finder.OrderBy;

import java.util.*;


public class AggregateList implements List<AggregateData>
{
    private Operation operation;
    private Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap = new UnifiedMap<String, MithraAggregateAttribute>(5);
    private Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap = new UnifiedMap<String, MithraGroupByAttribute>(5);
    private List<AggregateData> aggregateDataList;
    private com.gs.fw.common.mithra.HavingOperation havingClauseOperation;
    private boolean bypassCache;
    private OrderBy orderBy;


    public AggregateList(Operation operation)
    {
        this.operation = operation;
    }

    public void addAggregateAttribute(String name, com.gs.fw.common.mithra.MithraAggregateAttribute aggregateAttribute)
    {
        validateAttributeName(name);
        validateAttribute(aggregateAttribute.getTopLevelPortal());
        nameToAggregateAttributeMap.put(name,aggregateAttribute);
    }

    public void addGroupBy(String name, com.gs.fw.finder.Attribute attribute)
    {
        validateAttributeName(name);
        validateAttribute(((Attribute) attribute).getTopLevelPortal());
        GroupByAttribute groupByAttribute = new GroupByAttribute((Attribute) attribute);
        nameToGroupByAttributeMap.put(name, groupByAttribute);
    }

    public void setBypassCache(boolean bypassCache)
    {
        this.bypassCache = bypassCache;
    }

    public Set getAttributeAsSet(String attributeName)
    {
        Set result = new UnifiedSet(this.size());
        for(int i=0;i<this.size();i++)
        {
            result.add(this.get(i).getAttributeAsObject(attributeName));
        }
        return result;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public MutableIntSet getAttributeAsGscIntSet(String attributeName)
    {
        MutableIntSet result = new IntHashSet(this.size());
        for (int i = 0; i < this.size(); i++)
        {
            result.add(this.get(i).getAttributeAsInt(attributeName));
        }
        return result;
    }

    public org.eclipse.collections.api.set.primitive.MutableIntSet getAttributeAsEcIntSet(String attributeName)
    {
        org.eclipse.collections.api.set.primitive.MutableIntSet result = new org.eclipse.collections.impl.set.mutable.primitive.IntHashSet(this.size());
        for (int i = 0; i < this.size(); i++)
        {
            result.add(this.get(i).getAttributeAsInt(attributeName));
        }
        return result;
    }

    private void validateAttribute(MithraObjectPortal portal)
    {
        checkResolved();
        if (operation.getResultObjectPortal() != portal)
        {
            throw new MithraBusinessException("unexpected top level operation on object " + portal.getFinder().getClass().getName());
        }
    }

    private void validateHavingOperation(HavingOperation havingOperation)
    {
        checkResolved();
        if(havingOperation.getResultObjectPortal() != operation.getResultObjectPortal())
        {
            throw new MithraBusinessException("unexpected top level operation on object " + havingOperation.getResultObjectPortal().getFinder().getClass().getName());
        }
    }

    private void checkResolved()
    {
        if (this.isAggregateListOperationResolved())
        {
            throw new MithraBusinessException("Aggregate list is unmodifiable after retrieval. Can not add Aggregate attribute, GroupBy attribute, or Having Operation to a list that has been resolved");
        }
    }

    private void validateAttributeName(String attributeName)
    {
        if (nameToAggregateAttributeMap.containsKey(attributeName))
        {
            throw new MithraBusinessException("Aggregate list already contains an aggregate attribute with name: "+attributeName+".\n"+
            "An AggregateList cannot contain more than one attribute (AggregateAttribute or GroupByAttribute) with the same name");
        }

        if (nameToGroupByAttributeMap.containsKey(attributeName))
        {
            throw new MithraBusinessException("Aggregate list already contains a group by attribute with name: "+attributeName+".\n"+
            "An AggregateList cannot contain more than one attribute (AggregateAttribute or GroupByAttribute) with the same name");
        }
    }

    public void setHavingOperation(com.gs.fw.common.mithra.HavingOperation havingOperation)
    {
        validateHavingOperation(havingOperation);
        this.havingClauseOperation = havingOperation;
    }

    private Operation getOperation()
    {
        return operation;
    }

    private com.gs.fw.common.mithra.HavingOperation getHavingOperation()
    {
        return this.havingClauseOperation;
    }

    public int size()
    {
        return this.resolveOperation().size();
    }

    public boolean isEmpty()
    {
        return this.resolveOperation().isEmpty();
    }

    public boolean contains(Object o)
    {
        return this.resolveOperation().contains(o);
    }

    public Iterator<AggregateData> iterator()
    {
        return this.resolveOperation().iterator();
    }

    public Object[] toArray()
    {
        return this.resolveOperation().toArray();
    }

    public boolean add(AggregateData o)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean remove(Object o)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean addAll(Collection<? extends AggregateData> c)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean addAll(int index, Collection<? extends AggregateData> c)
    {
        throw createExceptionForModificationAttempt();
    }

    public void clear()
    {
        throw createExceptionForModificationAttempt();
    }

    public AggregateData get(int index)
    {
        return this.resolveOperation().get(index);
    }

    public AggregateData set(int index, AggregateData element)
    {
        throw createExceptionForModificationAttempt();
    }

    public void add(int index, AggregateData element)
    {
        throw createExceptionForModificationAttempt();
    }

    public AggregateData remove(int index)
    {
        throw createExceptionForModificationAttempt();
    }

    public int indexOf(Object o)
    {
        return this.resolveOperation().indexOf(o);
    }

    public int lastIndexOf(Object o)
    {
        return this.resolveOperation().lastIndexOf(o);
    }

    public ListIterator<AggregateData> listIterator()
    {
        return this.resolveOperation().listIterator();
    }

    public ListIterator<AggregateData> listIterator(int index)
    {
        return this.resolveOperation().listIterator(index);
    }

    public List<AggregateData> subList(int fromIndex, int toIndex)
    {
        return this.resolveOperation().subList(fromIndex, toIndex);
    }

    public boolean retainAll(Collection c)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean removeAll(Collection c)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean containsAll(Collection c)
    {
        return this.resolveOperation().containsAll(c);
    }

    public Object[] toArray(Object[] a)
    {
        return this.resolveOperation().toArray(a);
    }

    private MithraBusinessException createExceptionForModificationAttempt()
    {
        return new MithraBusinessException("Aggregate list is unmodifiable.");
    }

    private boolean isAggregateListOperationResolved()
    {
        return aggregateDataList != null;
    }

    private synchronized List<AggregateData> resolveOperation()
    {
        if (!this.isAggregateListOperationResolved())
        {
            aggregateDataList = this.getOperation().getResultObjectPortal().findAggregatedData(this.getOperation(),
                    this.nameToAggregateAttributeMap, this.nameToGroupByAttributeMap, this.getHavingOperation(), orderBy, this.bypassCache);
        }

        return aggregateDataList;
    }

    public void forceResolve()
    {
        this.resolveOperation();
    }

    public AggregateData getAggregateDataAt(int i)
    {
        return this.resolveOperation().get(i);
    }

    /**
     * The attribute to group by should be either a aggregate attribute or a group by attribute.
     *
     * @param attributeName
     */
    private void validateOrderByAttribute(String attributeName)
    {

        if ((!nameToAggregateAttributeMap.containsKey(attributeName)) && (!nameToGroupByAttributeMap.containsKey(attributeName)))
        {
            throw new MithraBusinessException("Aggregate list cannot be order by  attribute with name: " + attributeName + ".\n" +
                    "An AggregateList can only be ordered by an attribute which is either a AggregateAttribute or a GroupByAttribute");
        }

    }

    public void setAscendingOrderBy(String... names)
    {
        for (String name : names)
        {
            addOrderBy(name, true);
        }
        if (this.isAggregateListOperationResolved())
        {
            Collections.sort(aggregateDataList, orderBy);
        }
    }


    public void setDescendingOrderBy(String... names)
    {
        for (String name : names)
        {
            addOrderBy(name, false);
        }
        if (this.isAggregateListOperationResolved())
        {
            Collections.sort(aggregateDataList, orderBy);
        }
    }


    public void addOrderBy(String name, boolean isAscending)
    {
        validateOrderByAttribute(name);
        AggregateOrderBy orderBy = new AggregateOrderBy(name, isAscending);
        if (this.orderBy != null)
        {
            this.orderBy = this.orderBy.and(orderBy);
        }
        else
        {
            this.orderBy = orderBy;
        }

        if(this.isAggregateListOperationResolved())
        {
            Collections.sort(aggregateDataList, this.orderBy);
        }
    }
}

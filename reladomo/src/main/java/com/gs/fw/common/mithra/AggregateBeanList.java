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

import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.aggregate.AggregateBeanOrderBy;
import com.gs.fw.common.mithra.aggregate.attribute.BeanAggregateAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.finder.OrderBy;

import java.lang.reflect.Method;
import java.util.*;



public class AggregateBeanList<Bean> implements List<Bean>
{
    private Operation operation;
    private Class bean;
    private List aggregateBeanList;
    private Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap = new UnifiedMap<String, MithraAggregateAttribute>(5);
    private Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap = new UnifiedMap<String, MithraGroupByAttribute>(5);
    private com.gs.fw.common.mithra.HavingOperation havingClauseOperation;
    private boolean bypassCache;
    private OrderBy orderBy;

    private static Map<Class, Class> primitiveClassLookupMap;


    static
    {
        primitiveClassLookupMap = new UnifiedMap<Class, Class>(8);
        primitiveClassLookupMap.put(Byte.class, Byte.TYPE);
        primitiveClassLookupMap.put(Short.class, Short.TYPE);
        primitiveClassLookupMap.put(Integer.class, Integer.TYPE);
        primitiveClassLookupMap.put(Long.class, Long.TYPE);
        primitiveClassLookupMap.put(Float.class, Float.TYPE);
        primitiveClassLookupMap.put(Double.class, Double.TYPE);
        primitiveClassLookupMap.put(Boolean.class, Boolean.TYPE);
        primitiveClassLookupMap.put(Character.class, Character.TYPE);
    }

    public AggregateBeanList(Operation operation, Class bean)
    {
        this.operation = operation;
        this.bean = bean;
    }


    public void addAggregateAttribute(String name, MithraAggregateAttribute aggregateAttribute)
    {
        validateAttributeName(name);
        validateAttribute(aggregateAttribute.getTopLevelPortal());
        Method setterMethod = getSetterBeanMethod(name, aggregateAttribute.valueType());
        BeanAggregateAttribute beanAggregateAttribute = new BeanAggregateAttribute((AggregateAttribute) aggregateAttribute, setterMethod);
        nameToAggregateAttributeMap.put(name, beanAggregateAttribute);

    }

    public void addGroupBy(String name, Attribute attribute)
    {
        validateAttributeName(name);
        validateAttribute(attribute.getTopLevelPortal());
        Method method = getSetterBeanMethod(name, attribute.valueType());
        GroupByBeanAttribute groupByAttribute = new GroupByBeanAttribute(attribute, method);
        nameToGroupByAttributeMap.put(name, groupByAttribute);

    }

    private Method getSetterBeanMethod(String name, Class parameterType)
    {
        if (name.length() < 1)
        {
            throw new MithraBusinessException("name must not be empty");
        }
        String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
        try
        {
            return bean.getMethod(methodName, new Class[]{parameterType});
        }
        catch (NoSuchMethodException e)
        {

            try
            {
                Class primitiveParameterType = primitiveClassLookupMap.get(parameterType);
                return bean.getMethod(methodName, new Class[]{primitiveParameterType});
            }
            catch (NoSuchMethodException e1)
            {
                throw new MithraBusinessException("Method " + methodName + ", not found in the class " + bean.getName() , e1);
            }

        }
    }

    private Method getGetterBeanMethod(String name, Class parameterType)
    {
        if (name.length() < 1)
        {
            throw new MithraBusinessException("name must not be empty");
        }
        String methodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
        try
        {

            return bean.getMethod(methodName, null);

        }
        catch (NoSuchMethodException e)
        {
            throw new MithraBusinessException("Method " + methodName + ", not found in class " + bean.getName() , e);

        }
    }


    public void setBypassCache(boolean bypassCache)
    {
        this.bypassCache = bypassCache;
    }

    private void validateAttributeName(String attributeName)
    {
        if (nameToAggregateAttributeMap.containsKey(attributeName))
        {
            throw new MithraBusinessException("Aggregate list already contains an aggregate attribute with name: " + attributeName + ".\n" +
                    "An AggregateList cannot contain more than one attribute (AggregateAttribute or GroupByAttribute) with the same name");
        }

        if (nameToGroupByAttributeMap.containsKey(attributeName))
        {
            throw new MithraBusinessException("Aggregate list already contains a group by attribute with name: " + attributeName + ".\n" +
                    "An AggregateList cannot contain more than one attribute (AggregateAttribute or GroupByAttribute) with the same name");
        }
    }

    public void setHavingOperation(com.gs.fw.common.mithra.HavingOperation havingOperation)
    {
        validateHavingOperation(havingOperation);
        this.havingClauseOperation = havingOperation;
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
        if (havingOperation.getResultObjectPortal() != operation.getResultObjectPortal())
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

    private com.gs.fw.common.mithra.HavingOperation getHavingOperation()
    {
        return this.havingClauseOperation;
    }

    private synchronized List resolveOperation()
    {
        if (!this.isAggregateListOperationResolved())
        {
            aggregateBeanList = this.operation.getResultObjectPortal().findAggregatedBeanData(this.operation,
                    this.nameToAggregateAttributeMap, this.nameToGroupByAttributeMap, getHavingOperation(), orderBy, this.bypassCache, bean);
        }
        return aggregateBeanList;

    }

    public void forceResolve()
    {
        this.resolveOperation();
    }

    private boolean isAggregateListOperationResolved()
    {
        return aggregateBeanList != null;
    }

    public int size()
    {
        return this.resolveOperation().size();
    }


    public void setAscendingOrderBy(String... names)
    {
        for (String name : names)
        {
            addOrderBy(name, true);
        }
        if (this.isAggregateListOperationResolved())
        {
            Collections.sort(aggregateBeanList, orderBy);
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
            Collections.sort(aggregateBeanList, orderBy);
        }
    }


    public void addOrderBy(String name, boolean isAscending)
    {
        validateOrderByAttribute(name);
        Method getterMethod = getGetterBeanMethod(name, null);
        AggregateBeanOrderBy orderBy = new AggregateBeanOrderBy(getterMethod, isAscending);
        if (this.orderBy != null)
        {
            this.orderBy = this.orderBy.and(orderBy);
        }
        else
        {
            this.orderBy = orderBy;
        }
    }

    private void validateOrderByAttribute(String attributeName)
    {
        if ((!nameToAggregateAttributeMap.containsKey(attributeName)) && (!nameToGroupByAttributeMap.containsKey(attributeName)))
        {
            throw new MithraBusinessException("Aggregate bean list cannot be ordered by attribute with name: " + attributeName + ".\n" +
                    "An AggregateBeanList can only be ordered by an attribute which is either a AggregateAttribute or a GroupByAttribute");
        }
    }

    public boolean isEmpty()
    {
        return this.resolveOperation().isEmpty();
    }

    public boolean add(Object o)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean remove(Object o)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean addAll(Collection c)
    {
        throw createExceptionForModificationAttempt();
    }

    public boolean addAll(int index, Collection c)
    {
        throw createExceptionForModificationAttempt();
    }

    public void clear()
    {
        throw createExceptionForModificationAttempt();
    }

    public Bean get(int index)
    {
        return (Bean) this.resolveOperation().get(index);
    }

    public Bean set(int index, Object element)
    {
        throw createExceptionForModificationAttempt();
    }

    public void add(int index, Object element)
    {
        throw createExceptionForModificationAttempt();
    }

    public Bean remove(int index)
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

    public ListIterator<Bean> listIterator()
    {
        return this.resolveOperation().listIterator();
    }

    public ListIterator<Bean> listIterator(int index)
    {
        return this.resolveOperation().listIterator(index);
    }

    public List<Bean> subList(int fromIndex, int toIndex)
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

    public boolean contains(Object o)
    {
        return this.resolveOperation().contains(o);
    }

    public Iterator<Bean> iterator()
    {
        return this.resolveOperation().iterator();
    }

    public Object[] toArray()
    {
        return this.resolveOperation().toArray();
    }

    public Object[] toArray(Object[] a)
    {
        return this.resolveOperation().toArray(a);
    }

    private MithraBusinessException createExceptionForModificationAttempt()
    {
        return new MithraBusinessException("Aggregate Bean list is unmodifiable.");
    }


}

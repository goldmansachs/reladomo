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

package com.gs.reladomo.metadata;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.util.ReflectionMethodCache;
import com.gs.fw.common.mithra.util.serializer.DeserializationClassMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

public abstract class ReladomoClassMetaData
{
    private static final Object[] NULL_ARGS = (Object[]) null;
    protected static final Object NONE = new Object();
    private static final Function<? super Class,? extends ReladomoClassMetaData> META_DATA_FACTORY = new Function<Class, ReladomoClassMetaData>()
    {
        @Override
        public ReladomoClassMetaData valueOf(Class aClass)
        {
            return new PrivateReladomoClassMetaData(aClass);
        }
    };

    private static Logger logger = LoggerFactory.getLogger(ReladomoClassMetaData.class.getName());

    private static final ConcurrentHashMap<Class, ReladomoClassMetaData> CACHE = new ConcurrentHashMap<Class, ReladomoClassMetaData>();
    private final Function<? super String, ? extends Method> RELATIONSHIP_SETTER_LOOKUP = new RelationshipSetterLookup();

    private final Class finderClass;
    private final RelatedFinder relatedFinder;
    private final ConcurrentHashMap<String, Method> relationshipSetters = new ConcurrentHashMap<String, Method>();

    private Object processingDate;
    private Object businessDate;
    private String businessOrInterfaceClassName;
    private Class businessOrInterfaceClass;
    private Class businessImplClass;
    private Class onHeapDataClass;
    private int numberOfDatedDimensions = -1;

    private final ReladomoRuntimeMetaFunction metaFunction;

    protected ReladomoClassMetaData(Class finderClass)
    {
        this.finderClass = finderClass;
        this.relatedFinder = (RelatedFinder) invokeStaticMethod(this.finderClass, "getFinderInstance");
        this.metaFunction = new ReladomoRuntimeMetaFunction(this);
    }

    protected ReladomoClassMetaData(RelatedFinder finder)
    {
        //for temp tuple objects
        this.relatedFinder = finder;
        this.finderClass = this.relatedFinder.getClass();
        this.metaFunction = new ReladomoRuntimeMetaFunction(this);
    }

    public static ReladomoClassMetaData fromFinder(RelatedFinder finder)
    {
        return fromFinderClassName(finder.getFinderClassName());
    }

    public static ReladomoClassMetaData fromFinderClass(Class finderClass)
    {
        return CACHE.getIfAbsentPutWithKey(finderClass, META_DATA_FACTORY);
    }

    public static ReladomoClassMetaData fromFinderClassName(String finderClassName)
    {
        Class finderClass = null;
        try
        {
            finderClass = Class.forName(finderClassName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not find class for "+finderClassName, e);
        }
        return CACHE.getIfAbsentPutWithKey(finderClass, META_DATA_FACTORY);
    }

    public static ReladomoClassMetaData fromBusinessClass(Class businessClass)
    {
        return fromBusinessClassName(businessClass.getName());
    }

    public static ReladomoClassMetaData fromObjectInstance(MithraObject object)
    {
        return object.zGetPortal().getClassMetaData();
    }

    public static ReladomoClassMetaData fromListInstance(MithraList list)
    {
        return ((DelegatingList) list).getMithraObjectPortal().getClassMetaData();
    }

    public Attribute getOptimisticKeyFromAsOfAttributes()
    {
        AsOfAttribute processingDateAttribute = this.getProcessingDateAttribute();
        if (processingDateAttribute != null)
        {
            return processingDateAttribute.getFromAttribute();
        }
        return null;
    }

    /**
     *
      * @param businessClassName the canonical object name, declared in the xml. For objects with generated interface, this
     *                          will may include Impl at the end (but it's better if it doesn't).
     * @return Metadata for this class
     */
    public static ReladomoClassMetaData fromBusinessClassName(String businessClassName)
    {
        Class finderClass = null;
        try
        {
            finderClass = Class.forName(businessClassName+"Finder");
        }
        catch (ClassNotFoundException e)
        {
            if (businessClassName.endsWith("Impl"))
            {
                try
                {
                    finderClass = Class.forName(businessClassName.substring(0, businessClassName.length() - "Impl".length())+"Finder");
                }
                catch (ClassNotFoundException e1)
                {
                    throw new RuntimeException("Could not find finder class for "+businessClassName, e1);
                }
            }
            else
            {
                throw new RuntimeException("Could not find finder class for "+businessClassName, e);
            }
        }
        return CACHE.getIfAbsentPutWithKey(finderClass, META_DATA_FACTORY);
    }

    public ReladomoRuntimeMetaFunction getMetaFunction()
    {
        return metaFunction;
    }

    public boolean isDated()
    {
        return this.getAsOfAttributes() != null;
    }

    public int getNumberOfDatedDimensions()
    {
        int result = this.numberOfDatedDimensions;
        if (result == -1)
        {
            AsOfAttribute[] asOfAttributes = getAsOfAttributes();
            result = asOfAttributes == null ? 0 : asOfAttributes.length;
            this.numberOfDatedDimensions = result;
        }
        return result;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.relatedFinder.getAsOfAttributes();
    }

    public AsOfAttribute getProcessingDateAttribute()
    {
        Object result = this.processingDate;
        if (result == null)
        {
            result = findProcessingDate();
            this.processingDate = result;
        }
        if (result == NONE)
        {
            return null;
        }
        return (AsOfAttribute) result;
    }

    private Object findProcessingDate()
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        if (asOfAttributes == null)
        {
            return NONE;
        }
        for(int i=0;i<asOfAttributes.length;i++)
        {
            if (asOfAttributes[i].isProcessingDate())
            {
                return asOfAttributes[i];
            }
        }
        return NONE;
    }

    public AsOfAttribute getBusinessDateAttribute()
    {
        Object result = this.businessDate;
        if (result == null)
        {
            result = findBusinessDate();
            this.businessDate = result;
        }
        if (result == NONE)
        {
            return null;
        }
        return (AsOfAttribute) result;
    }

    private Object findBusinessDate()
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        if (asOfAttributes == null)
        {
            return NONE;
        }
        for(int i=0;i<asOfAttributes.length;i++)
        {
            if (!asOfAttributes[i].isProcessingDate())
            {
                return asOfAttributes[i];
            }
        }
        return NONE;
    }

    public boolean hasGeneratedInterface()
    {
        return this.getBusinessOrInterfaceClass() != this.getBusinessImplClass();
    }

    public String getBusinessImplClassName()
    {
        return getBusinessImplClass().getName();
    }

    public String getBusinessOrInterfaceClassName()
    {
        String bizOrInterfaceName = this.businessOrInterfaceClassName;
        if (bizOrInterfaceName == null)
        {
            bizOrInterfaceName = findBusinessOrInterfaceClassName();
            this.businessOrInterfaceClassName = bizOrInterfaceName;
        }
        return bizOrInterfaceName;
    }

    private String findBusinessOrInterfaceClassName()
    {
        return getFinderClassName().substring(0, getFinderClassName().length() - "Finder".length());
    }

    public Class getBusinessImplClass()
    {
        Class bizImpl = this.businessImplClass;
        if (bizImpl == null)
        {
            bizImpl = findBusinessImplClass();
            this.businessImplClass = bizImpl;
        }
        return bizImpl;
    }

    private Class findBusinessImplClass()
    {
        Class bizOrInterface = this.getBusinessOrInterfaceClass();
        if (bizOrInterface.isInterface())
        {
            try
            {
                return Class.forName(getBusinessOrInterfaceClassName()+"Impl");
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Could not find class for object "+getBusinessOrInterfaceClassName(), e);
            }
        }
        else
        {
            return bizOrInterface;
        }
    }

    public Class getBusinessOrInterfaceClass()
    {
        Class bizClass = this.businessOrInterfaceClass;
        if (bizClass == null)
        {
            bizClass = findBusinessOrInterfaceClass();
            this.businessOrInterfaceClass = bizClass;
        }
        return bizClass;
    }

    private Class findBusinessOrInterfaceClass()
    {
        try
        {
            return Class.forName(getBusinessOrInterfaceClassName());
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not find class for object "+getBusinessOrInterfaceClassName(), e);
        }
    }

    public Class getOnHeapDataClass()
    {
        // 1
        Class dataClass = this.onHeapDataClass;
        if (dataClass == null)
        {
            dataClass = findOnHeapDataClass();
            this.onHeapDataClass = dataClass;
        }
        return dataClass;
    }

    private Class findOnHeapDataClass()
    {
        try
        {
            Class dataClass = Class.forName(getBusinessOrInterfaceClassName()+"Data");
            if (dataClass.isInterface())
            {
                dataClass = Class.forName(dataClass.getName()+"$"+dataClass.getSimpleName()+"OnHeap");
            }
            return dataClass;
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not find data class for object "+getBusinessOrInterfaceClassName(), e);
        }
    }

    public RelatedFinder getFinderInstance()
    {
        return this.relatedFinder;
    }

    public boolean isTemporaryObject()
    {
        return this.relatedFinder.isTemporary();
    }

    public Class getFinderClass()
    {
        return this.finderClass;
    }

    public String getFinderClassName()
    {
        return relatedFinder.getFinderClassName();
    }

    public int getSerialVersionId()
    {
        return relatedFinder.getSerialVersionId();
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return relatedFinder.getSourceAttributeType();
    }

    public Attribute getSourceAttribute()
    {
        return relatedFinder.getSourceAttribute();
    }

    public Attribute[] getPrimaryKeyAttributes()
    {
        return relatedFinder.getPrimaryKeyAttributes();
    }

    public Attribute[] getPersistentAttributes()
    {
        return relatedFinder.getPersistentAttributes();
    }

    public VersionAttribute getVersionAttribute()
    {
        return relatedFinder.getVersionAttribute();
    }

    public List<RelatedFinder> getRelationshipFinders()
    {
        return relatedFinder.getRelationshipFinders();
    }

    public List<RelatedFinder> getDependentRelationshipFinders()
    {
        return relatedFinder.getDependentRelationshipFinders();
    }

    public Attribute getAttributeByName(String attributeName)
    {
        return relatedFinder.getAttributeByName(attributeName);
    }

    public RelatedFinder getRelationshipFinderByName(String relationshipName)
    {
        return relatedFinder.getRelationshipFinderByName(relationshipName);
    }

    public Function getAttributeOrRelationshipSelector(String attributeName)
    {
        return relatedFinder.getAttributeOrRelationshipSelector(attributeName);
    }

    public boolean isPure()
    {
        return relatedFinder.isPure();
    }

    public int getHierarchyDepth()
    {
        return relatedFinder.getHierarchyDepth();
    }

    public Method getRelationshipSetter(String name)
    {
        return relationshipSetters.getIfAbsentPutWith(name, RELATIONSHIP_SETTER_LOOKUP, name);
    }

    protected static Object invokeStaticMethod(Class classToInvoke, String methodName)
    {
        try
        {
            Method method = ReflectionMethodCache.getZeroArgMethod(classToInvoke, methodName);
            return method.invoke(null, NULL_ARGS);
        }
        catch (Exception e)
        {
            logger.error("Could not invoke method "+methodName+" on class "+classToInvoke, e);
            throw new MithraException("Could not invoke method "+methodName+" on class "+classToInvoke, e);
        }
    }

    private class RelationshipSetterLookup implements Function<String, Method>
    {
        @Override
        public Method valueOf(String name)
        {
            Method[] methods = ReladomoClassMetaData.this.getBusinessOrInterfaceClass().getMethods();
            for(Method m: methods)
            {
                if (m.getName().startsWith("set") &&
                        Character.toLowerCase(m.getName().charAt(3)) == Character.toLowerCase(name.charAt(0))  &&
                        m.getName().substring(4).equals(name.substring(1)) && m.getParameterTypes().length == 1)
                {
                    return m;
                }
            }
            return null;
        }
    }
}

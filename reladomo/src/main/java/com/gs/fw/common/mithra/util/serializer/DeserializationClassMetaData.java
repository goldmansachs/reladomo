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

package com.gs.fw.common.mithra.util.serializer;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

public class DeserializationClassMetaData
{
    private static final Class[][] CONSTRUCTOR_ARGS = new Class[3][];
    private static final Object[] NO_ARGS = new Object[0];
    private final Function<? super String, ? extends Method> RELATIONSHIP_SETTER_LOOKUP = new RelationshipSetterLookup();

    private final RelatedFinder relatedFinder;
    private final boolean isTemporal;
    private final int numTempralArgs;
    private final Class businessClass;
    private final Class dataClass;
    private final ObjectIntHashMap<Attribute> attributePosition = new ObjectIntHashMap<Attribute>();
    private final AsOfAttribute businessDateAttribute;
    private final AsOfAttribute processingDateAttribute;
    private final Attribute sourceAttribute;
    private final Attribute[] pkAttributesNoSource;
    private Constructor businessClassConstructor;
    private final List<Attribute> settableAttributes = FastList.newList();
    private final Set<String> dependentRelationshipNames = UnifiedSet.newSet();
    private ConcurrentHashMap<String, Method> relationshipSetters = new ConcurrentHashMap<String, Method>();

    static
    {
        CONSTRUCTOR_ARGS[1] = new Class[1];
        CONSTRUCTOR_ARGS[1][0] = Timestamp.class;

        CONSTRUCTOR_ARGS[2] = new Class[2];
        CONSTRUCTOR_ARGS[2][0] = Timestamp.class;
        CONSTRUCTOR_ARGS[2][1] = Timestamp.class;
    }

    public DeserializationClassMetaData(RelatedFinder relatedFinder)
    {
        this.relatedFinder = ((AbstractRelatedFinder) relatedFinder).zWithoutParent();
        String className = getBusinessClassName();
        this.businessClass = findBusinessClass(className);
        String dataClassName = className+"Data";
        try
        {
            Class dataClassTmp = Class.forName(dataClassName);
            if (dataClassTmp.isInterface())
            {
                dataClassTmp = Class.forName(dataClassTmp.getName()+"$"+dataClassTmp.getSimpleName()+"OnHeap");
            }
            this.dataClass = dataClassTmp;
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not get class "+className, e);
        }
        int count = 0;
        for(Attribute attr: this.relatedFinder.getPersistentAttributes())
        {
            attributePosition.put(attr, count);
            settableAttributes.add(attr);
            count++;
        }
        AsOfAttribute[] asOfAttributes = this.relatedFinder.getAsOfAttributes();
        AsOfAttribute localProc = null;
        AsOfAttribute localBiz = null;
        this.isTemporal = asOfAttributes != null;
        this.numTempralArgs = asOfAttributes != null ? asOfAttributes.length : 0;
        if (this.isTemporal)
        {
            for(AsOfAttribute attr: asOfAttributes)
            {
                attributePosition.put(attr, count);
                count++;
                if (attr.isProcessingDate())
                {
                    localProc = attr;
                }
                else
                {
                    localBiz = attr;
                }
            }
        }
        Attribute[] pks = this.relatedFinder.getPrimaryKeyAttributes();
        this.sourceAttribute = this.relatedFinder.getSourceAttribute();
        if (this.sourceAttribute != null)
        {
            attributePosition.put(this.sourceAttribute, count);
            count++;
            settableAttributes.add(this.sourceAttribute);
            pks = new Attribute[pks.length - 1];
            List<Attribute> localPks = FastList.newList(pks.length);
            for(Attribute a: this.relatedFinder.getPrimaryKeyAttributes())
            {
                if (!a.equals(sourceAttribute))
                {
                    localPks.add(a);
                }
            }
            localPks.toArray(pks);
        }
        pkAttributesNoSource = pks;
        businessDateAttribute = localBiz;
        processingDateAttribute = localProc;
        initBusinessObjectConstructor();
        List<RelatedFinder> dependentRelationshipFinders = this.relatedFinder.getDependentRelationshipFinders();
        for(int i=0;i<dependentRelationshipFinders.size();i++)
        {
            this.dependentRelationshipNames.add(((AbstractRelatedFinder)dependentRelationshipFinders.get(i)).getRelationshipName());
        }
    }

    private Class findBusinessClass(String className)
    {
        try
        {
            Class<?> aClass = Class.forName(className);
            if (aClass.isInterface())
            {
                aClass = Class.forName(className+"Impl");
            }
            return aClass;
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not get class "+className, e);
        }
    }

    public Set<String> getDependentRelationshipNames()
    {
        return dependentRelationshipNames;
    }

    public Method getRelationshipSetter(String name)
    {
        return relationshipSetters.getIfAbsentPutWith(name, RELATIONSHIP_SETTER_LOOKUP, name);
    }

    private void initBusinessObjectConstructor()
    {
        try
        {
            if (isTemporal)
            {
                this.businessClassConstructor = this.businessClass.getConstructor(CONSTRUCTOR_ARGS[this.numTempralArgs]);
            }
            else
            {
                this.businessClassConstructor = this.businessClass.getConstructor(null);
            }
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException("Could not find constructor for "+businessClass.getName());
        }
    }

    public List<Attribute> getSettableAttributes()
    {
        return settableAttributes;
    }

    private String getBusinessClassName()
    {
        String finderClassName = relatedFinder.getFinderClassName();
        return finderClassName.substring(0, finderClassName.length() - "Finder".length());
    }

    public RelatedFinder getRelatedFinder()
    {
        return relatedFinder;
    }

    public MithraDataObject constructData() throws DeserializationException
    {
        try
        {
            return (MithraDataObject) dataClass.newInstance();
        }
        catch (Exception e)
        {
            throw new DeserializationException("Could not instantiate object for class "+dataClass.getName(), e);
        }
    }

    public Attribute getAttributeByName(String attributeName)
    {
        return relatedFinder.getAttributeByName(attributeName);
    }

    public RelatedFinder getRelationshipFinderByName(String relationshipName)
    {
        return relatedFinder.getRelationshipFinderByName(relationshipName);
    }

    public Method getAnnotatedMethodByName(String name)
    {
        return DeserializableMethodCache.getInstance().get(businessClass, name);
    }

    public int getTotalAttributes()
    {
        return attributePosition.size();
    }

    public int getAttriutePosition(Attribute attribute)
    {
        return attributePosition.getIfAbsent(attribute, -1);
    }

    public boolean isConfigured()
    {
        return MithraManagerProvider.getMithraManager().getConfigManager().isClassConfigured(getBusinessClassName());
    }

    @Override
    public int hashCode()
    {
        return this.relatedFinder.getFinderClassName().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof DeserializationClassMetaData)
        {
            return this.relatedFinder.getFinderClassName().equals(((DeserializationClassMetaData)obj).getRelatedFinder().getFinderClassName());
        }
        return false;
    }

    public AsOfAttribute getBusinessDateAttribute()
    {
        return businessDateAttribute;
    }

    public AsOfAttribute getProcessingDateAttribute()
    {
        return processingDateAttribute;
    }

    public Attribute getSourceAttribute()
    {
        return sourceAttribute;
    }

    public Attribute[] getPrimaryKeyAttributesWithoutSource()
    {
        return pkAttributesNoSource;
    }

    public MithraObject constructObject(Timestamp businessDate, Timestamp processingDate) throws DeserializationException
    {
        if (processingDateAttribute != null && processingDate == null)
        {
            processingDate = processingDateAttribute.getDefaultDate();
        }
        if (businessDateAttribute != null && businessDate == null)
        {
            businessDate = businessDateAttribute.getDefaultDate();
        }
        try
        {
            switch (numTempralArgs)
            {
                case 0:
                    return (MithraObject) businessClassConstructor.newInstance(NO_ARGS);
                case 1:
                    if (processingDateAttribute != null)
                    {
                        return (MithraObject) businessClassConstructor.newInstance(processingDate);
                    }
                    return (MithraObject) businessClassConstructor.newInstance(businessDate);
                case 2:
                    return (MithraObject) businessClassConstructor.newInstance(businessDate, processingDate);
            }
        }
        catch (Exception e)
        {
            throw new DeserializationException("Could not construct "+businessClass.getName(), e);
        }
        return null; // never gets here.
    }

    private class RelationshipSetterLookup implements Function<String, Method>
    {
        @Override
        public Method valueOf(String name)
        {
            Method[] methods;
            try
            {
                methods = Class.forName(DeserializationClassMetaData.this.getBusinessClassName()).getMethods();
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Could not get methods for class "+DeserializationClassMetaData.this.getBusinessClassName(), e);
            }
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

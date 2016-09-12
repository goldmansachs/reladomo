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

package com.gs.fw.common.mithra.generator;

import com.gs.fw.common.mithra.generator.type.DoubleJavaType;
import com.gs.fw.common.mithra.generator.type.JavaType;
import com.gs.fw.common.mithra.generator.type.BigDecimalJavaType;
import com.gs.fw.common.mithra.generator.util.StringUtility;
import com.gs.fw.common.mithra.generator.metamodel.AttributeEmbeddedValueType;
import com.gs.fw.common.mithra.generator.metamodel.MithraEmbeddedValueObjectType;
import com.gs.fw.common.mithra.generator.metamodel.ReferenceEmbeddedValueType;
import com.gs.fw.common.mithra.generator.metamodel.RelationshipType;

import java.util.*;


public class MithraEmbeddedValueObjectTypeWrapper extends MithraBaseObjectTypeWrapper
{
    private Set<String> imports = new HashSet<String>();
    private List<AttributeWrapper> attributes = new ArrayList<AttributeWrapper>();
    private List<NestedObjectWrapper> nestedObjects = new ArrayList<NestedObjectWrapper>();
    private List<AttributeWrapper> nullablePrimitiveAttributes = new ArrayList<AttributeWrapper>();
    private List<RelationshipAttribute> relationshipAttributes = new ArrayList<RelationshipAttribute>();

    public MithraEmbeddedValueObjectTypeWrapper(MithraEmbeddedValueObjectType wrapped, String sourceFileName, String importedSource)
    {
        super(wrapped, sourceFileName, importedSource);
        super.createAuxiliaryClassNames();
        this.extractAttributes();
        this.extractNestedObjects();
    }

    public List<RelationshipType> getRelationships()
    {
        return getWrapped().getRelationships();
    }

    public List<RelationshipAttribute> getRelationshipAttributes()
    {
        return relationshipAttributes;
    }

    public void setRelationshipAttributes(List<RelationshipAttribute> rels)
    {
        this.relationshipAttributes = rels;
    }

    public void addToRequiredClasses(String packageName, String className)
    {
        if (!this.getPackageName().equals(packageName))
        {
            this.imports.add(packageName + "." + className);
        }
    }

    public Set<String> getImports()
    {
        return Collections.unmodifiableSet(this.imports);
    }

    public MithraEmbeddedValueObjectType getWrapped()
    {
        return (MithraEmbeddedValueObjectType) super.getWrapped();
    }

    public MithraEmbeddedValueObjectTypeWrapper getSuperClassWrapper()
    {
        return (MithraEmbeddedValueObjectTypeWrapper) super.getSuperClassWrapper();
    }

    public String getDescription()
    {
        return "embedded value object";
    }

    public String getObjectType()
    {
        return "embedded-value";
    }

    public boolean isTablePerSubclassSuperClass()
    {
        return false;
    }

    public AttributeWrapper getAttribute(String name)
    {
        for (AttributeWrapper wrapper : this.attributes)
        {
            if (wrapper.getName().equals(name))
            {
                return wrapper;
            }
        }
        return null;
    }

    public AttributeWrapper[] getAttributes()
    {
        AttributeWrapper[] attributes = new AttributeWrapper[this.attributes.size()];
        return this.attributes.toArray(attributes);
    }

    public AttributeWrapper[] getSortedAttributes()
    {
        AttributeWrapper[] attributes = this.getAttributes();
        Arrays.sort(attributes);
        return attributes;
    }

    public NestedObjectWrapper[] getNestedObjects()
    {
        NestedObjectWrapper[] nestedObjects = new NestedObjectWrapper[this.nestedObjects.size()];
        return this.nestedObjects.toArray(nestedObjects);
    }

    public NestedObjectWrapper[] getSortedNestedObjects()
    {
        NestedObjectWrapper[] nestedObjects = this.getNestedObjects();
        Arrays.sort(nestedObjects);
        return nestedObjects;
    }

    public NestedObjectWrapper getNestedObjectByName(String name)
    {
        for (NestedObjectWrapper wrapper : this.nestedObjects)
        {
            if (wrapper.getName().equals(name))
            {
                return wrapper;
            }
        }
        return null;
    }

    public String getNullGetterExpressionForAttribute(AttributeWrapper attribute)
    {
        return super.getNullGetterExpressionForIndex(this.findIndexForAttribute(attribute));
    }

    public String getNullSetterExpressionForAttribute(AttributeWrapper attribute)
    {
        return super.getNullSetterExpressionForIndex(this.findIndexForAttribute(attribute));
    }

    public String getNotNullSetterExpressionForAttribute(AttributeWrapper attribute)
    {
        return getNotNullSetterExpressionForIndex(this.findIndexForAttribute(attribute));
    }

    private int findIndexForAttribute(AttributeWrapper attribute)
    {
        int index = -1;
        for (int i = 0; i < nullablePrimitiveAttributes.size(); i++)
        {
            AttributeWrapper candidate = this.nullablePrimitiveAttributes.get(i);
            if (candidate.getName().equals(attribute.getName()))
            {
                index = i;
                break;
            }
        }
        return index;
    }

    public List<String> resolveNestedEmbeddedValueObjects(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects)
    {
        //TODO: pass around the real errorMessages list
        List<String> errorMessages = new ArrayList<String>();
        for (NestedObjectWrapper nestedObject : this.getSortedNestedObjects())
        {
            String name = nestedObject.getTypeAsString();
            MithraEmbeddedValueObjectTypeWrapper wrapper = allEmbeddedValueObjects.get(name);
            nestedObject.resolveAttributes(wrapper.getSortedAttributes());
            nestedObject.resolveNestedObjects(wrapper.getSortedNestedObjects());
        }
        return errorMessages;
    }

    private void extractAttributes()
    {
        for (int i = 0; i < this.getWrapped().getAttributes().size(); i++)
        {
            AttributeEmbeddedValueType attribute = (AttributeEmbeddedValueType) this.getWrapped().getAttributes().get(i);
            AttributeWrapper attributeWrapper = new AttributeWrapper(attribute);
            this.attributes.add(attributeWrapper);
            if (attributeWrapper.isPrimitive())
            {
                this.nullablePrimitiveAttributes.add(attributeWrapper);
            }
        }
        this.initializeNullBitHolders();
    }

    private void initializeNullBitHolders()
    {
        super.initializeNullBitHolders(this.nullablePrimitiveAttributes.size());
    }

    private void extractNestedObjects()
    {
        for (int i = 0; i < this.getWrapped().getEmbeddedValues().size(); i++)
        {
            ReferenceEmbeddedValueType nestedObject = (ReferenceEmbeddedValueType) this.getWrapped().getEmbeddedValues().get(i);
            this.nestedObjects.add(new NestedObjectWrapper(nestedObject));
        }
    }

    public static abstract class AbstractWrapper
    {
        public String getExtractor()
        {
            return this.getName() + "Extractor";
        }

        public String getNullGetter()
        {
            return "is" + StringUtility.firstLetterToUpper(this.getName()) + "Null";
        }

        public String getNullSetter()
        {
            return "set" + StringUtility.firstLetterToUpper(this.getName()) + "Null";
        }

        public String getGetter()
        {
            return "get" + StringUtility.firstLetterToUpper(this.getName());
        }

        public String getSetter()
        {
            return "set" + StringUtility.firstLetterToUpper(this.getName());
        }

        public String getSetterUntil()
        {
            return this.getSetter() + "Until";
        }

        public String getCopyValuesFrom()
        {
            return "copy" + StringUtility.firstLetterToUpper(this.getName());
        }

        public String getCopyValuesFromUntil()
        {
            return this.getCopyValuesFrom() + "Until";
        }

        public String getExtractionMethodName()
        {
            return StringUtility.firstLetterToLower(this.getTypeAsString()) + "ValueOf";
        }

        public String getValueSetterMethodName()
        {
            return "set" + StringUtility.firstLetterToUpper(this.getTypeAsString()) + "Value";
        }

        public String getValueSetterUntilMethodName()
        {
            return this.getValueSetterMethodName() + "Until";
        }

        public abstract String getName();
        public abstract String getTypeAsString();
    }

    public static class AttributeWrapper extends AbstractWrapper implements Comparable
    {
        private AttributeEmbeddedValueType wrapped;
        private JavaType type;

        public AttributeWrapper(AttributeEmbeddedValueType wrapped)
        {
            this.wrapped = wrapped;
            this.type = JavaType.create(this.wrapped.getJavaType());
        }

        public String getName()
        {
            return this.wrapped.getName();
        }

        private JavaType getType()
        {
            return this.type;
        }

        public String getTypeAsString()
        {
            return this.getType().getJavaTypeString();
        }

        public String getTypeAsStringPrimary()
        {
            return this.getType().getJavaTypeStringPrimary();
        }

        public boolean isPrimitive()
        {
            return this.getType().isPrimitive();
        }

        public boolean isDoubleAttribute()
        {
            return this.getType() instanceof DoubleJavaType;
        }

        public boolean isBigDecimalAttribute()
        {
            return this.getType() instanceof BigDecimalJavaType;
        }

        public String getIncrementer()
        {
            return "increment" + StringUtility.firstLetterToUpper(this.getName());
        }

        public String getIncrementerUntil()
        {
            return this.getIncrementer() + "Until";
        }

        public String getExtractorClassName()
        {
            return StringUtility.firstLetterToUpper(this.getTypeAsString()) + "Extractor";
        }

        public String getAttributeClassName()
        {
            return this.getTypeAsStringPrimary() + "Attribute";
        }

        public String getObjectComparisonString(String o1, String o2)
        {
            return this.getType().getObjectComparisonString(o1, o2);
        }

        public String getPrimitiveComparisonString(String p1, String p2)
        {
            return this.getType().getPrimitiveComparisonString(p1, p2);
        }

        public int compareTo(Object o)
        {
            AttributeWrapper other = (AttributeWrapper) o;
            return this.getName().compareTo(other.getName());
        }
    }

    public static class NestedObjectWrapper extends AbstractWrapper implements Comparable
    {
        private ReferenceEmbeddedValueType wrapped;
        private List<AttributeWrapper> attributes;
        private List<NestedObjectWrapper> nestedObjects;

        public NestedObjectWrapper(ReferenceEmbeddedValueType wrapped)
        {
            this.wrapped = wrapped;
        }

        public String getName()
        {
            return this.wrapped.getName();
        }

        public String getTypeAsString()
        {
            return this.wrapped.getType();
        }

        public String getExtractorClassName()
        {
            return this.getTypeAsString() + "Extractor";
        }

        public String getAttributeClassName()
        {
            return this.getTypeAsString() + "Attribute";
        }

        public String getQualifiedAttributeClassName()
        {
            return this.getTypeAsString() + "." + this.getAttributeClassName();
        }

        public AttributeWrapper[] getAttributes()
        {
            AttributeWrapper[] attributes = new AttributeWrapper[this.attributes.size()];
            return this.attributes.toArray(attributes);
        }

        public NestedObjectWrapper[] getNestedObjects()
        {
            NestedObjectWrapper[] nestedObjects = new NestedObjectWrapper[this.nestedObjects.size()];
            return this.nestedObjects.toArray(nestedObjects);
        }

        public void resolveAttributes(AttributeWrapper[] attributes)
        {
            this.attributes = new ArrayList<AttributeWrapper>(attributes.length);
            for (AttributeWrapper attribute : attributes)
            {
                this.attributes.add(attribute);
            }
        }

        public void resolveNestedObjects(NestedObjectWrapper[] nestedObjects)
        {
            this.nestedObjects = new ArrayList<NestedObjectWrapper>(nestedObjects.length);
            for (NestedObjectWrapper nestedObject : nestedObjects)
            {
                this.nestedObjects.add(nestedObject);
            }
        }

        public int compareTo(Object o)
        {
            NestedObjectWrapper other = (NestedObjectWrapper) o;
            return this.getName().compareTo(other.getName());
        }
    }
}

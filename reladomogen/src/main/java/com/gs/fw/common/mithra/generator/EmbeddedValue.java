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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.gs.fw.common.mithra.generator.metamodel.EmbeddedValueMappingType;
import com.gs.fw.common.mithra.generator.metamodel.EmbeddedValueType;
import com.gs.fw.common.mithra.generator.metamodel.NestedEmbeddedValueType;
import com.gs.fw.common.mithra.generator.metamodel.RelationshipType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

public class EmbeddedValue implements Comparable
{

    private final MithraObjectTypeWrapper owner;
    private final NestedEmbeddedValueType wrapped;
    private int hierarchyDepth;
    private String type;
    private List<EmbeddedValueMapping> mappings;
    private List<EmbeddedValue> children;
    private List<EmbeddedValue> ancestors;
    private List<RelationshipAttribute> relationshipAttributes;

    public EmbeddedValue(MithraObjectTypeWrapper owner, NestedEmbeddedValueType wrapped)
    {
        this(owner, wrapped, null);
    }

    private EmbeddedValue(MithraObjectTypeWrapper owner, NestedEmbeddedValueType wrapped, EmbeddedValue parent)
    {
        this.owner = owner;
        this.wrapped = wrapped;
        if (this.wrapped instanceof EmbeddedValueType)
        {
            this.initRoot();
        }
        else
        {
            this.initNested(parent);
        }
        this.mappings = this.extractMappings();
        this.children = this.extractNestedObjects();
    }
    
    public boolean isFinalGetter()
    {
        return this.wrapped.isFinalGetterSet() ? this.wrapped.isFinalGetter() : this.owner.isDefaultFinalGetters();
    }

    public List<RelationshipAttribute> getRelationshipAttributes()
    {
        return relationshipAttributes;
    }

    public boolean isRoot()
    {
        return this.getHierarchyDepth() == 0;
    }

    public boolean isNested()
    {
        return !this.isRoot();
    }

    public int getHierarchyDepth()
    {
        return this.hierarchyDepth;
    }

    public String getType()
	{
		return this.type;
	}

    public void setType(String type)
    {
        this.type = type;
    }

    public EmbeddedValueMapping[] getMappings()
	{
        EmbeddedValueMapping[] attributes = new EmbeddedValueMapping[this.mappings.size()];
        return this.mappings.toArray(attributes);
	}

    public EmbeddedValueMapping[] getMappingsRecursively()
    {
        List<EmbeddedValueMapping> recursiveAttributes = new ArrayList<EmbeddedValueMapping>();
        recursiveAttributes.addAll(this.mappings);
        for (EmbeddedValue nestedObject : this.getDescendants())
        {
            recursiveAttributes.addAll(nestedObject.mappings);
        }
        EmbeddedValueMapping[] attributes = new EmbeddedValueMapping[recursiveAttributes.size()];
        return recursiveAttributes.toArray(attributes);
    }

    public EmbeddedValue[] getChildren()
    {
        EmbeddedValue[] children = new EmbeddedValue[this.children.size()];
        return this.children.toArray(children);
    }

    public EmbeddedValue[] getAncestors()
    {
        EmbeddedValue[] ancestors = new EmbeddedValue[this.ancestors.size()];
        return this.ancestors.toArray(ancestors);
    }

    public EmbeddedValue[] getDescendants()
    {
        List<EmbeddedValue> descendants = new ArrayList<EmbeddedValue>();
        for (EmbeddedValue child : this.getChildren())
        {
            descendants.add(child);
            descendants.addAll(Arrays.asList(child.getDescendants()));
        }
        EmbeddedValue[] descendantsArray = new EmbeddedValue[descendants.size()];
        return descendants.toArray(descendantsArray);
    }

    public String getName()
	{
		return this.wrapped.getName();
	}

    public String getNestedName()
    {
        String nestedName = "";
        for (EmbeddedValue ancestor : this.ancestors)
        {
            nestedName += StringUtility.firstLetterToUpper(ancestor.getName());
        }
        nestedName += StringUtility.firstLetterToUpper(this.getName());
        return StringUtility.firstLetterToLower(nestedName);
    }

    public String getFactoryMethodName()
    {
        return "managedInstance";
    }

    public String getGetter()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getName());
    }

    public String getSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName());
    }

    public String getCopyValuesFrom()
    {
        return "copy" + StringUtility.firstLetterToUpper(this.getName());
    }

    public String getNestedGetter()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getNestedName());
    }

    public String getNestedSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getNestedName());
    }

    public String getNestedCopyValuesFrom()
    {
        return "copy" + StringUtility.firstLetterToUpper(this.getNestedName());
    }

    public String getNestedCopyValuesFromUntil()
    {
        return this.getNestedCopyValuesFrom() + "Until";
    }

    public String getChainedInvocation()
    {
        String chainedInvocation = "";
        for (EmbeddedValue ancestor : this.ancestors)
        {
            chainedInvocation += ancestor.getName() + "().";
        }
        return chainedInvocation + this.getName();
    }

    public String getChainedGetter()
    {
       return this.getChainedPrefix() + this.getGetter();
    }

    public String getChainedGetterAfterDepth(int hierarchyDepth)
    {
        return this.getChainedPrefixAfterDepth(hierarchyDepth) + this.getGetter();
    }

    public String getChainedSetter()
    {
        return this.getChainedPrefix() + this.getSetter();
    }

    public String getChainedCopyValuesFrom()
    {
        return this.getChainedPrefix() + this.getCopyValuesFrom();
    }

    private String getChainedPrefix()
    {
        String chainedPrefix = "";
        for (EmbeddedValue ancestor : this.ancestors)
        {
            chainedPrefix += ancestor.getGetter() + "().";
        }
        return chainedPrefix;
    }

    private String getChainedPrefixAfterDepth(int hierarchyDepth)
    {
        String chainedPrefix = "";
        for (int i = hierarchyDepth + 1; i < this.ancestors.size(); i++)
        {
            chainedPrefix += this.ancestors.get(i).getGetter() + "().";
        }
        return chainedPrefix;
    }

    public String getAttributeClassName()
    {
        return this.getType() + "Attribute";
    }

    public String getQualifiedAttributeClassName()
    {
        return this.getType() + ".Managed" + this.getAttributeClassName();
    }

    public String getAttributeWrapperClassName()
    {
        return this.getType() + "Attributes";
    }

    public String getExtractorClassName()
    {
        return this.getType() + "Extractor";
    }

    public String getQualifiedExtractorClassName()
    {
        return this.getType() + this.getExtractorClassName();
    }

    public String getExtractorValueOf()
    {
        return StringUtility.firstLetterToLower(this.getType()) + "ValueOf";
    }

    public String getExtractorSetValue()
    {
        return "set" + this.getType() + "Value";
    }

    public String getExtractorSetValueUntil()
    {
        return this.getExtractorSetValue() + "Until";
    }

    public String getVisibility()
    {
        return isRoot() ? "public" : "protected";
    }

    public int compareTo(Object o)
    {
        if (o instanceof EmbeddedValue)
        {
            EmbeddedValue other = (EmbeddedValue) o;
            return this.getNestedName().compareTo(other.getNestedName());
        }
        return 0;
    }

    public void resolveTypes(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects, Map<String, MithraObjectTypeWrapper> mithraObjects)
    {
        this.resolveMappings(allEmbeddedValueObjects);
        this.resolveNestedObjects(allEmbeddedValueObjects);
        this.resolveRelationships(allEmbeddedValueObjects, mithraObjects);
    }

    private void resolveRelationships(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects, Map<String, MithraObjectTypeWrapper> mithraObjects)
    {
        MithraEmbeddedValueObjectTypeWrapper wrapper = allEmbeddedValueObjects.get(this.getType());
        List<RelationshipAttribute> relationshipsForType = new ArrayList<RelationshipAttribute>();
        for(RelationshipType rel: wrapper.getRelationships())
        {
            MithraObjectTypeWrapper toObject = mithraObjects.get(rel.getRelatedObject());
            this.owner.addRelationship(constructRelationship(rel, toObject));
            relationshipsForType.add(new RelationshipAttribute(this.owner, toObject, rel));
            wrapper.addToRequiredClasses(toObject.getPackageName(), toObject.getClassName());
            wrapper.addToRequiredClasses(toObject.getPackageName(), toObject.getFinderClassName());
        }
        wrapper.setRelationshipAttributes(relationshipsForType);
        this.relationshipAttributes = relationshipsForType;
    }

    public String getAliasedRelationshipName(RelationshipAttribute rel)
    {
        return this.getNestedName()+StringUtility.firstLetterToUpper(rel.getName());
    }

    private RelationshipAttribute constructRelationship(RelationshipType rel, MithraObjectTypeWrapper toObject)
    {
        if (toObject == null)
        {
            throw new RuntimeException("Could not resolve related object "+rel.getRelatedObject()+" in relationship "+rel.getName()+" in embedded value object "
                + this.getType());
        }
        String name = this.getNestedName()+StringUtility.firstLetterToUpper(rel.getName());
        StringBuilder builder = new StringBuilder(rel.value());
        int index = builder.indexOf("this.");
        while(index >= 0)
        {
            builder.setCharAt(index + "this.".length(), Character.toUpperCase(builder.charAt(index+"this.".length())));
            index = builder.indexOf("this.", index + 1);
        }
        String query = builder.toString();
        query = query.replace("this.", "this."+getNestedName());
        RelationshipType impl = new RelationshipType();
        impl.setCardinality(rel.getCardinality());
        impl.setDirectReference(rel.isDirectReference());
        impl.setForeignKey(rel.getForeignKey());
        impl.setName(name);
        impl.setOrderBy(rel.getOrderBy());
        impl.setParameters(rel.getParameters());
        impl.setRelatedIsDependent(rel.isRelatedIsDependent());
        impl.setRelatedObject(rel.getRelatedObject());
        impl.setReturnType(rel.getReturnType());
        impl.setReverseRelationshipName(rel.getReverseRelationshipName());
        impl._setValue(query);
        return new RelationshipAttribute(this.owner, toObject, impl);
    }

    protected void resolveMappings(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects)
    {
        MithraEmbeddedValueObjectTypeWrapper wrapper = allEmbeddedValueObjects.get(this.getType());
        for (EmbeddedValueMapping mapping : this.getMappings())
        {
            String attributeName = mapping.getMappingName();
            String javaType = wrapper.getAttribute(attributeName).getTypeAsString();
            mapping.setJavaType(javaType);
            if (!this.owner.hasMappingToColumn(mapping.getColumnName()))
            {
                this.owner.addAttribute(mapping);
            }
            if (!wrapper.getPackageName().equals(this.owner.getPackageName()))
            {
                this.owner.addToRequiredClasses(wrapper.getPackageName(), wrapper.getClassName());
            }
        }
        this.owner.initializeNullBitHolders();
    }

    protected void resolveNestedObjects(Map<String, MithraEmbeddedValueObjectTypeWrapper> allEmbeddedValueObjects)
    {
        MithraEmbeddedValueObjectTypeWrapper wrapper = allEmbeddedValueObjects.get(this.getType());
        for (EmbeddedValue nestedObject : this.getChildren())
        {
            MithraEmbeddedValueObjectTypeWrapper.NestedObjectWrapper nestedWrapper = wrapper.getNestedObjectByName(nestedObject.getName());
            String type = nestedWrapper.getTypeAsString();
            nestedObject.setType(type);
            nestedObject.resolveMappings(allEmbeddedValueObjects);
            nestedObject.resolveNestedObjects(allEmbeddedValueObjects);
        }
    }

    private void initRoot()
    {
        this.hierarchyDepth = 0;
        this.setType(((EmbeddedValueType) this.wrapped).getType());
        this.ancestors = new ArrayList<EmbeddedValue>(0);
    }

    private void initNested(EmbeddedValue parent)
    {
        this.hierarchyDepth = parent.getHierarchyDepth() + 1;
        this.ancestors = new ArrayList<EmbeddedValue>(parent.getAncestors().length + 1);
        this.ancestors.addAll(Arrays.asList(parent.getAncestors()));
        this.ancestors.add(parent);
    }

    private List<EmbeddedValueMapping> extractMappings()
    { 
        int numMappings = this.wrapped.getMappings().size();
        List<EmbeddedValueMapping> mappings = new ArrayList<EmbeddedValueMapping>(numMappings);
        for (int i = 0; i < numMappings; i++)
        {
            EmbeddedValueMappingType mappingType = (EmbeddedValueMappingType) this.wrapped.getMappings().get(i);
            EmbeddedValueMapping mapping = new EmbeddedValueMapping(mappingType, this.owner, this);
            mappings.add(mapping);
        }
        return mappings;
    }

    private List<EmbeddedValue> extractNestedObjects()
    {
        int numNestedObjects = this.wrapped.getEmbeddedValues().size();
        List<EmbeddedValue> nestedObjects = new ArrayList<EmbeddedValue>(numNestedObjects);
        for (int i = 0; i < numNestedObjects; i++)
        {
            NestedEmbeddedValueType embeddedValueType = (NestedEmbeddedValueType) this.wrapped.getEmbeddedValues().get(i);
            EmbeddedValue embeddedValue = new EmbeddedValue(this.owner, embeddedValueType, this);
            nestedObjects.add(embeddedValue);
        }
        return nestedObjects;
    }
}

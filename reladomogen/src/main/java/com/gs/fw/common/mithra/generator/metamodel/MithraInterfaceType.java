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

package com.gs.fw.common.mithra.generator.metamodel;

import com.gs.fw.common.mithra.generator.CommonWrapper;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;

import java.util.*;


public class MithraInterfaceType extends MithraInterfaceTypeAbstract implements CommonWrapper
{
    private String sourceFileName;

    private boolean readOnlyInterfaces = false;
    private String importedSource;

    private Set<MithraInterfaceType> allSuperInterfaces = new HashSet<MithraInterfaceType>();
    private Map<String, AttributeInterfaceType> allSuperAttributes = new HashMap<String, AttributeInterfaceType>();
    private Map<String, AsOfAttributeInterfaceType> allSuperAsOfAttributes = new HashMap<String, AsOfAttributeInterfaceType>();
    private Map<String, RelationshipInterfaceType> allSuperRelationships = new HashMap<String, RelationshipInterfaceType>();
    private Map<String, AttributeInterfaceType> declaredAttributes = new HashMap<String, AttributeInterfaceType>();
    private Map<String, AsOfAttributeInterfaceType> declaredAsOfAttributes = new HashMap<String, AsOfAttributeInterfaceType>();
    private Map<String, RelationshipInterfaceType> declaredRelationships = new HashMap<String, RelationshipInterfaceType>();

    public MithraInterfaceType()
    {
        super();
    }

    public String getDescription()
    {
        return "mithra-interface";
    }

    public String getImplClassName()
    {
        return this.getClassName();
    }

    public String getAbstractClassName()
    {
        return this.getClassName() + "Abstract";
    }

    public List<String> extractRelationshipsAndSuperInterfaces(Map<String, MithraInterfaceType> mithraInterfaces, Map<String,
            MithraObjectTypeWrapper> mithraObjects)
    {
        List<String> errorMessages = new ArrayList<String>();
        for (int i = 0; i < this.getRelationships().size(); i++)
        {
            RelationshipInterfaceType relationshipType = this.getRelationships().get(i);
            relationshipType.resolveRelatedObject(mithraInterfaces, mithraObjects, errorMessages);

            this.addToRequiredClasses(relationshipType.getRelatedMithraObject().getPackageName(),
                    relationshipType.getRelatedMithraObject().getClassName() + "Finder");

            this.addToRequiredClasses(relationshipType.getRelatedMithraObject().getPackageName(),
                    relationshipType.getRelatedMithraObject().getClassName());
        }
        this.extractSuperInterfaces(mithraInterfaces, errorMessages);

        return errorMessages;
    }

    public void addToRequiredClasses(String packageName, String className)
    {
        if (!this.getPackageName().equals(packageName))
        {
            this.getImports().add(packageName + "." + className);
        }
    }

    public void extractSuperInterfaces(Map<String, MithraInterfaceType> mithraInterfaces, List<String> errorMessages)
    {
        List<String> superInterfaces = this.getSuperInterfaces();
        for (int i = 0; i < superInterfaces.size(); i++)
        {
            MithraInterfaceType superMithraInterface = mithraInterfaces.get(superInterfaces.get(i));

            HashSet<MithraInterfaceType> currentAllSuper = new HashSet<MithraInterfaceType>();
            List<String> errors = new ArrayList<String>();
            currentAllSuper.add(superMithraInterface);
            HashSet<MithraInterfaceType> allSuperCurrentInterface = processSuperInterfaces(mithraInterfaces, superMithraInterface, currentAllSuper, errors);
            if (errors.size() > 0)
            {
                errorMessages.addAll(errors);
                return;
            }
            else
            {
                allSuperInterfaces.addAll(allSuperCurrentInterface);
            }

        }
        if (errorMessages.size() == 0)
        {
            initializeSuperInterfaceAttributesAndRelationships(errorMessages);
        }
    }

    public HashSet<MithraInterfaceType> processSuperInterfaces(Map<String, MithraInterfaceType> mithraInterfaces, MithraInterfaceType mithraInterfaceType,
                                                               HashSet<MithraInterfaceType> accumulator, List<String> errorMessages)
    {
        if (errorMessages.size() == 0)
        {
            List<String> superInterfaces = mithraInterfaceType.getSuperInterfaces();
            for (int i = 0; i < superInterfaces.size(); i++)
            {
                MithraInterfaceType superMithraInterface = mithraInterfaces.get(superInterfaces.get(i));
                if (accumulator.contains(superMithraInterface))
                {
                    errorMessages.add("Circular dependency between SuperInterface : " + superMithraInterface.getClassName() + " and  " + this.getClassName());
                    break;
                }
                else
                {
                    accumulator.add(superMithraInterface);
                    processSuperInterfaces(mithraInterfaces, superMithraInterface, accumulator, errorMessages);
                }
            }
        }
        return accumulator;
    }


    private void initializeSuperInterfaceAttributesAndRelationships(List<String> errorMessages)
    {
        for (MithraInterfaceType superInterface : allSuperInterfaces)
        {
            for (AttributeInterfaceType superInterfaceType : superInterface.getAttributes())
            {
                AttributeInterfaceType localAttributeType = declaredAttributes.get(superInterfaceType.getName());
                if (localAttributeType != null && localAttributeType.isCompatibleWithSuperAttribute(superInterfaceType, errorMessages))
                {
                    allSuperAttributes.put(superInterfaceType.getName(), superInterfaceType);
                }
            }

            for (AsOfAttributeInterfaceType superAsOfAttribute : superInterface.getAsOfAttributes())
            {
                AsOfAttributeInterfaceType localAsOfAttribute = declaredAsOfAttributes.get(superAsOfAttribute.getName());
                if (localAsOfAttribute != null && localAsOfAttribute.isCompatibleWithSuperAsOfAttribute(superAsOfAttribute, errorMessages))
                {
                    allSuperAsOfAttributes.put(superAsOfAttribute.getName(), superAsOfAttribute);
                }
            }

            SourceAttributeInterfaceType superSourceAttributeType = superInterface.getSourceAttribute();
            SourceAttributeInterfaceType localSourceAttributeType = this.getSourceAttribute();
            if (localSourceAttributeType != null && superSourceAttributeType != null)
            {
                localSourceAttributeType.isCompatibleWithSuperSourceAttribute(superSourceAttributeType, errorMessages);
            }

            for (RelationshipInterfaceType superRelationshipType : superInterface.getRelationships())
            {
                RelationshipInterfaceType localRelationshipType = declaredRelationships.get(superRelationshipType.getName());
                if (localRelationshipType != null && localRelationshipType.isCompatibleWithSuperRelationship(superRelationshipType, errorMessages))
                {
                    allSuperRelationships.put(superRelationshipType.getName(), superRelationshipType);
                }
            }
        }

    }


    private void initializeAttributes()
    {
        for (AttributeInterfaceType attributeInterfaceType : this.getAttributes())
        {
            declaredAttributes.put(attributeInterfaceType.getName(), attributeInterfaceType);
        }
        for (AsOfAttributeInterfaceType asOfAttributeInterfaceType : this.getAsOfAttributes())
        {
            declaredAsOfAttributes.put(asOfAttributeInterfaceType.getName(), asOfAttributeInterfaceType);
        }
    }

    private void initializeRelationships()
    {
        for (RelationshipInterfaceType relationshipInterfaceType : this.getRelationships())
        {
            declaredRelationships.put(relationshipInterfaceType.getName(), relationshipInterfaceType);
        }
    }

    public RelationshipInterfaceType[] getAllRelationships()
    {
        Map<String, RelationshipInterfaceType> allRelationships = new HashMap<String, RelationshipInterfaceType>();
        allRelationships.putAll(declaredRelationships);
        allRelationships.putAll(allSuperRelationships);
        RelationshipInterfaceType[] allRelationshipInterfaceType = new RelationshipInterfaceType[allRelationships.size()];
        return allRelationships.values().toArray(allRelationshipInterfaceType);
    }


    public AttributeInterfaceType[] getAllAttributes()
    {
        Map<String, AttributeInterfaceType> allAttributes = new HashMap<String, AttributeInterfaceType>();
        allAttributes.putAll(declaredAttributes);
        allAttributes.putAll(allSuperAttributes);
        AttributeInterfaceType[] allAttributeInterfaceType = new AttributeInterfaceType[allAttributes.size()];
        return allAttributes.values().toArray(allAttributeInterfaceType);
    }

    public AsOfAttributeInterfaceType[] getAllAsOfAttributes()
    {
        Map<String, AsOfAttributeInterfaceType> allAsOfAttributes = new HashMap<String, AsOfAttributeInterfaceType>();
        allAsOfAttributes.putAll(declaredAsOfAttributes);
        allAsOfAttributes.putAll(allSuperAsOfAttributes);
        AsOfAttributeInterfaceType[] allAsOfAttributeInterfaceType = new AsOfAttributeInterfaceType[allAsOfAttributes.size()];
        return allAsOfAttributes.values().toArray(allAsOfAttributeInterfaceType);
    }

    public AttributeInterfaceType[] getAttributesAsArray()
    {
        AttributeInterfaceType[] attributesArray = new AttributeInterfaceType[declaredAttributes.size()];
        return declaredAttributes.values().toArray(attributesArray);
    }

    public AsOfAttributeInterfaceType[] getAsOfAttributesAsArray()
    {
        AsOfAttributeInterfaceType[] attributes = new AsOfAttributeInterfaceType[this.getAsOfAttributes().size()];
        return this.getAsOfAttributes().toArray(attributes);
    }

    public boolean hasSuperInterface()
    {
        return (this.allSuperInterfaces != null && this.allSuperInterfaces.size() > 0);
    }

    public Set<MithraInterfaceType> getAllSuperInterfaces()
    {
        return allSuperInterfaces;
    }

    public String getSuperInterfacesForAbstract()
    {
        StringBuilder result = new StringBuilder();
        for (String superInterface : this.getSuperInterfaces())
        {
            result.append(superInterface);
            result.append(", ");
        }

        String returnsStr = result.toString();
        int index = returnsStr.lastIndexOf(", ");
        if (index != -1)
        {
            returnsStr = result.substring(0, index);
        }
        return returnsStr;
    }


    public String getSuperInterfacesForFinder()
    {
        StringBuilder result = new StringBuilder();
        for (String superInterface : this.getSuperInterfaces())
        {

            result.append(superInterface).append("Finder<Result>");
            result.append(", ");
        }
        // remove the last comma.
        String returnsStr = result.toString();
        int index = returnsStr.lastIndexOf(", ");
        if (index != -1)
        {
            returnsStr = result.substring(0, index);
        }
        return returnsStr;
    }

    public RelationshipInterfaceType[] getRelationshipsAsArray()
    {
        RelationshipInterfaceType[] relationships = new RelationshipInterfaceType[this.getRelationships().size()];
        return this.getRelationships().toArray(relationships);
    }

    public boolean hasSourceAttribute()
    {
        return (this.getSourceAttribute() != null);
    }

    public void setSourceFileName(String sourceFile)
    {
        this.sourceFileName = sourceFile;
    }

    public String getSourceFileName()
    {
        return sourceFileName;
    }

    public void postInitialize(String objectName)
    {
        setSourceFileName(objectName);
        initializeAttributes();
        initializeRelationships();
    }

    public void setReadOnlyInterfaces(boolean readOnlyInterfaces)
    {
        this.readOnlyInterfaces = readOnlyInterfaces;
    }

    public boolean isReadOnlyInterfaces()
    {
        return this.readOnlyInterfaces;
    }

    public void setImportedSource(String importedSource)
    {
        this.importedSource = importedSource;
    }

    public boolean isImported()
    {
        return this.importedSource != null;
    }
}

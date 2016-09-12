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

import com.gs.fw.common.mithra.generator.Cardinality;
import com.gs.fw.common.mithra.generator.CommonWrapper;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.RelationshipAttribute;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public class RelationshipInterfaceType extends RelationshipInterfaceTypeAbstract
{
    private CommonWrapper relatedMithraObject;

    public Cardinality getCardinalityObject()
    {
        return Cardinality.getByName(this.getCardinality().value());
    }

    public String getParameters()
    {
        if (super.getParameters() != null)
        {
            return super.getParameters();
        }
        else
        {
            return "";
        }
    }

    public void resolveRelatedObject(Map<String, MithraInterfaceType> mithraInterfaces,
                                     Map<String, MithraObjectTypeWrapper> mithraObjects, List<String> errorMessages)
    {
        relatedMithraObject = mithraInterfaces.get(this.getRelatedObject());
        if (relatedMithraObject == null)
        {
            relatedMithraObject = mithraObjects.get(this.getRelatedObject());
        }
        if (relatedMithraObject == null)
        {
            errorMessages.add("The Relationship " + this.getName() + " has been defined  to have a related object name " + this.getRelatedObject() +
                    " which is neither a MithraObject nor a MithraInterface. RelatedObject of a MithraInterface Relationships needs to be a valid MithraObject or MithraInterface");
        }
    }

    public String getRelatedObjectClassNameForTemplate()
    {
        if (relatedMithraObject instanceof MithraObjectTypeWrapper)
        {
            MithraObjectTypeWrapper relatedObj = (MithraObjectTypeWrapper) relatedMithraObject;
            if (this.getCardinality().isManyToOne() || this.getCardinality().isOneToOne())
            {
                return relatedObj.getClassName() + "Finder." + relatedObj.getClassName() + "SingleFinderForRelatedClasses";
            }
            else
            {
                return relatedObj.getClassName() + "Finder." + relatedObj.getClassName() + "CollectionFinderForRelatedClasses";
            }
        }
        else
        {
            MithraInterfaceType relatedObj = (MithraInterfaceType) relatedMithraObject;
            return relatedObj.getClassName() + "Finder";
        }
    }

    public CommonWrapper getRelatedMithraObject()
    {
        return relatedMithraObject;
    }

    public boolean validateRelationship(Map<String, MithraInterfaceType> mithraInterfaces,
                                        RelationshipAttribute localRelationshipAttribute, String mithraObjectClassname,
                                        String mithraInterfaceName,
                                        List<String> errors)
    {
        if (localRelationshipAttribute == null)
        {
            errors.add("Object " + mithraObjectClassname + " is defined to implement the MithraInterface " + mithraInterfaceName
                    + " but does not define the relationship - " + this.getName());
            return false;
        }
        else if (!(isRelatedObjectAssignableFrom(localRelationshipAttribute.getRelatedObject(), mithraInterfaces, mithraObjectClassname, errors)))
        {
            return false;
        }
        else if (!this.getCardinalityObject().equals(localRelationshipAttribute.getCardinality()))
        {
            errors.add("Object " + mithraObjectClassname + " has a relationship " + this.getName() + " with cardinality " + localRelationshipAttribute.getCardinality() +
                    "which is not equal with the cardinality " + this.getCardinalityObject() + "of the mithrainterface " + mithraInterfaceName);

            return false;
        }
        else if (!this.getParameters().equals(localRelationshipAttribute.getParameters()))
        {
            errors.add("Object " + mithraObjectClassname + " has a relationship " + this.getName() + " with parameters " + localRelationshipAttribute.getParameters() +
                    "and is not equal to the parameters " + this.getParameters() + "of the mithrainterface " + mithraInterfaceName);
            return false;
        }
        return true;
    }

    public boolean isRelatedObjectAssignableFrom(MithraObjectTypeWrapper relatedObject, Map<String, MithraInterfaceType> mithraInterfaces,
                                                 String mithraObjectClassname, List<String> errors)
    {
        String localRelatedObjectName = this.getRelatedObject();
        String relatedObjectName = relatedObject.getWrapped().getClassName();
        if (localRelatedObjectName.equals(relatedObjectName))
        {
            return true;
        }

        List<String> relatedObjInterfaces = relatedObject.getWrapped().getMithraInterfaces();

        for (String interfaceName : relatedObjInterfaces)
        {
            if (localRelatedObjectName.equals(interfaceName) || checkSuperInterfaces(localRelatedObjectName, interfaceName, mithraInterfaces))
            {
                return true;
            }
        }

        errors.add("Object" + mithraObjectClassname + " has a relationship " + this.getName() + " with relatedObject name " + relatedObjectName +
                " which is not equal to " + localRelatedObjectName + " RelatedObjects must be the same or they must be another mithrainterface or a superinterface");
        return false;
    }

    private boolean checkSuperInterfaces(String localRelationshipName, String interfaceName, Map<String, MithraInterfaceType> mithraInterfaces)
    {
        MithraInterfaceType interfaceObject = mithraInterfaces.get(interfaceName);
        Collection<MithraInterfaceType> allSuperInterfaces = interfaceObject.getAllSuperInterfaces();

        for (MithraInterfaceType superInterface : allSuperInterfaces)
        {
            if (superInterface.getClassName().equals(localRelationshipName))
            {
                return true;
            }
        }

        return false;
    }

    public boolean isCompatibleWithSuperRelationship(RelationshipInterfaceType superRelationshipType, List<String> errors)
    {
        if (!(this.getRelatedObject().equals(superRelationshipType.getRelatedObject())))
        {
            errors.add("Inconsistent Relationship. The RelatedObject" + this.getRelatedObject() + " does not match the related object in SuperInterface. " +
                    "SuperInterface RelatedObject : " + superRelationshipType.getRelatedObject());
            return false;
        }
        else if (!this.getCardinalityObject().equals(superRelationshipType.getCardinalityObject()))
        {
            errors.add("Inconsistent Relationship. The Relationship cardinality " + this.getCardinalityObject() + " does not match the cardinality in SuperInterface. " +
                    "SuperInterface cardinality : " + superRelationshipType.getCardinalityObject());

            return false;
        }
        else if (!this.getParameters().equals(superRelationshipType.getParameters()))
        {
            errors.add("Inconsistent Relationship. The Relationship parameters " + this.getParameters() + " does not match the parameters in SuperInterface. " +
                    "SuperInterface parameters : " + superRelationshipType.getParameters());
            return false;
        }

        return true;
    }

    public String getGetter()
    {
        return "get"+ StringUtility.firstLetterToUpper(this.getName());
    }

    public boolean hasSetter()
    {
        return !this.hasParameters();
    }

    public boolean hasParameters()
    {
        return super.getParameters() != null && super.getParameters().trim().length() > 0;
    }

    public String getSetter()
    {
        return "set"+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getTypeAsString()
    {
        String className = this.relatedMithraObject.getClassName();
        String type = className;
        if (this.getCardinality().isOneToMany() || this.getCardinality().isManyToMany())
        {
            type = type + "List";
        }
        return StringUtility.trimPackage(type);
    }
}

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

import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.type.JavaType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.List;


public class AttributeInterfaceType extends AttributeInterfaceTypeAbstract
{

    private JavaType getType()
    {
        return JavaType.create(this.getJavaType());
    }

    public String getTypeAsString()
    {
        return this.getType().getJavaTypeString();
    }

    public String getTypeAsStringPrimary()
    {
        return this.getType().getJavaTypeStringPrimary();
    }

    public String getAttributeClassName()
    {
        return this.getTypeAsStringPrimary() + "Attribute";
    }

    public String getGetter()
    {
        return this.getType().getGetterPrefix()+StringUtility.firstLetterToUpper(this.getName());
    }

    public String getSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName());
    }

    public boolean isCompatibleWithSuperAttribute(AttributeInterfaceType attributeInterfaceType, List<String> errorMessages)
    {

        if (!isCompatible(attributeInterfaceType.getTypeAsString()))
        {
            errorMessages.add("Inconsistent Attribute Type.  " + this.getName() + " has a type " + this.getTypeAsString()
                    + " which is not consistent with its SuperInterface Type : " + attributeInterfaceType.getTypeAsString());
            return false;
        }

        return true;
    }

    public boolean validAttribute(Attribute localAttribute, String className, String mithraInterfaceName, List<String> errors)
    {
        if (!isCompatible(localAttribute.getTypeAsString()))
        {
            errors.add("Object " + className + " is defined to implement the MithraInterface " + mithraInterfaceName
                    + " but the attribute - " + this.getName() + " has either not been defined or has a incorrect type. ");
            return false;
        }
        return true;
    }


    private boolean isCompatible(String attributeTypeAsString)
    {
        return this.getTypeAsString().equals(attributeTypeAsString);
    }
}

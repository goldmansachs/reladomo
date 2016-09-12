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

import com.gs.fw.common.mithra.generator.SourceAttribute;
import com.gs.fw.common.mithra.generator.type.JavaType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.List;


public class SourceAttributeInterfaceType extends SourceAttributeInterfaceTypeAbstract
{
    private JavaType getType()
    {
        return JavaType.create(this.getJavaType());
    }

    public String getTypeAsString()
    {
        return this.getType().getJavaTypeString();
    }

    public String getGetter()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getName());
    }

    public String getSetter()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName());
    }

    public String getTypeAsStringPrimary()
    {
        return this.getType().getJavaTypeStringPrimary();
    }

    public String getAttributeClassName()
    {
        return this.getTypeAsStringPrimary() + "Attribute";
    }

    public boolean validAttribute(SourceAttribute sourceAttribute, String className, String mithraInterfaceName, List<String> errors)
    {

        if (!this.getName().equals(sourceAttribute.getName()) || !(this.getTypeAsString().equals(sourceAttribute.getType().getJavaTypeString())))
        {
            errors.add("Object " + className + " is defined to be implement the MithraInterface " + mithraInterfaceName
                    + " but the SourceAttribute - " + this.getName() + " has either not been defined or has a incorrect name or type. ");
            return false;
        }
        return true;

    }

    public void isCompatibleWithSuperSourceAttribute(SourceAttributeInterfaceType superSourceAttributeType, List<String> errorMessages)
    {
        if (!this.getName().equals(superSourceAttributeType.getName()) || !this.getTypeAsString().equals(superSourceAttributeType.getTypeAsString()))
        {
            errorMessages.add("Inconsistent SourceAttribute. " + this.getName() + " has a type " + this.getTypeAsString()
                    + " which is not consistent with its SuperInterface Type : " + superSourceAttributeType.getName() + ", " + superSourceAttributeType.getTypeAsString());

        }

    }
}

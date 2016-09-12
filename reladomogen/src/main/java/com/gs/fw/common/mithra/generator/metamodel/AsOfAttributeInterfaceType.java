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

import com.gs.fw.common.mithra.generator.AsOfAttribute;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.List;


public class AsOfAttributeInterfaceType extends AsOfAttributeInterfaceTypeAbstract
{
    public String getGetterFrom()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getName()) + "From";
    }

    public String getSetterFrom()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName()) + "From";
    }

    public String getGetterTo()
    {
        return "get" + StringUtility.firstLetterToUpper(this.getName()) + "To";
    }

    public String getSetterTo()
    {
        return "set" + StringUtility.firstLetterToUpper(this.getName()) + "To";
    }

    public TimezoneConversionType getTimezoneConversion()
    {
        TimezoneConversionType conversionType = super.getTimezoneConversion();
        if (conversionType == null)
        {
            conversionType = TimezoneConversionType.NONE;
        }
        return conversionType;
    }


    public boolean validAttribute(AsOfAttribute asOfAttribute, String className, String mithraInterfaceName, List<String> errors)
    {
        if (!(this.getInfinityDate().equals(asOfAttribute.getInfinityDate())) ||
                (this.isToIsInclusive() != asOfAttribute.isToIsInclusive()) ||
                (this.isIsProcessingDate() != asOfAttribute.isProcessingDate()) ||
                !(this.getTimezoneConversion().value().equals(asOfAttribute.getTimezoneConversion().value()))
                )
        {
            errors.add("Object " + className + " is defined to implement the MithraInterface " + mithraInterfaceName
                    + " but the AsOfAttribute - " + asOfAttribute.getName() + " has either not been defined or has a incorrect details. ");

            return false;
        }

        return true;
    }

    public boolean isCompatibleWithSuperAsOfAttribute(AsOfAttributeInterfaceType superAsOfAttribute, List<String> errorMessages)
    {

        if(!(this.getInfinityDate().equals(superAsOfAttribute.getInfinityDate())) ||
                (this.isToIsInclusive() != superAsOfAttribute.isToIsInclusive()) ||
                (this.isIsProcessingDate() != superAsOfAttribute.isIsProcessingDate()) ||
                !(this.getTimezoneConversion().value().equals(superAsOfAttribute.getTimezoneConversion().value())))
        {

            errorMessages.add("Inconsistent AsOfAttribute Type.  " + this.getName() + " has a following details " +
                    this.getInfinityDate() + ", " + this.isToIsInclusive() + ", " + this.isIsProcessingDate() + ", " + this.getTimezoneConversion().value()
                    + " which is not consistent with its SuperInterface AsOfAttribute details : " +
                    superAsOfAttribute.getInfinityDate() + ", " + superAsOfAttribute.isToIsInclusive() + ", " + superAsOfAttribute.isIsProcessingDate() + ", " + superAsOfAttribute.getTimezoneConversion().value());

            return false;
        }

       return true;
    }
}

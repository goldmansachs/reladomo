
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

import com.gs.fw.common.mithra.generator.metamodel.MithraObjectType;
import com.gs.fw.common.mithra.generator.metamodel.MithraPureObjectType;
import com.gs.fw.common.mithra.generator.metamodel.SourceAttributeType;

import java.util.List;

public class MithraPureObjectTypeAsObjectType extends MithraPureObjectType
{

    public SourceAttributeType getSourceAttribute()
    {
        return null;
    }

    public void setSourceAttribute(SourceAttributeType value)
    {
    }

    public boolean isSetSourceAttribute()
    {
        return false;
    }

    public void unsetSourceAttribute()
    {
    }

    public String getDefaultTable()
    {
        return null;
    }

    public void setDefaultTable(String value)
    {
    }

    public boolean isSetDefaultTable()
    {
        return false;
    }

    public void unsetDefaultTable()
    {
    }

    public List getEmbeddedValue()
    {
        return null;
    }

    public boolean isSetEmbeddedValue()
    {
        return false;
    }

    public void unsetEmbeddedValue()
    {
    }

    public List getEnumerationAttribute()
    {
        return null;
    }

    public boolean isSetEnumerationAttribute()
    {
        return false;
    }

    public void unsetEnumerationAttribute()
    {
    }
}

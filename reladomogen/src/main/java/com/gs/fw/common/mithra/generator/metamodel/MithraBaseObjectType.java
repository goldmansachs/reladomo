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

import java.util.*;
public class MithraBaseObjectType
 extends MithraBaseObjectTypeAbstract

{
    public boolean isDisableForeignKeys()
    {
        return false;
    }

    public SuperClassTypeAbstract getSuperClassType()
    {
        return null;
    }

    public ObjectType getObjectType()
    {
        return null;
    }

    public List<? extends RelationshipType> getRelationships()
    {
        return Collections.EMPTY_LIST;
    }

    public List<? extends AsOfAttributePureType> getAsOfAttributes()
    {
        return Collections.EMPTY_LIST;
    }

    public List getAttributes()
    {
        return Collections.EMPTY_LIST;
    }

    public List getComputedAttributes()
    {
        return Collections.EMPTY_LIST;
    }

    public SourceAttributeType getSourceAttribute()
    {
        return null;
    }

    public List<? extends TransactionalMethodSignatureType> getTransactionalMethodSignatures()
    {
        return Collections.EMPTY_LIST;
    }

    public String getDefaultTable()
    {
        return null;
    }

    public List<? extends IndexType> getIndexes()
    {
        return Collections.EMPTY_LIST;
    }

    public String getDatedTransactionalTemporalDirector()
    {
        return null;
    }

    public List getEmbeddedValues()
    {
        return Collections.EMPTY_LIST;
    }

    public List<String> getMithraInterfaces()
    {
        return Collections.EMPTY_LIST;
    }
}

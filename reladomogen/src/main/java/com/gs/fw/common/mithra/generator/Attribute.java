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

import com.gs.fw.common.mithra.generator.metamodel.AttributePureType;
import com.gs.fw.common.mithra.generator.type.JavaTypeException;
import com.gs.fw.common.mithra.generator.metamodel.AttributeType;


public class Attribute extends AbstractAttribute
{
    private String owningRelationshipName;
    private String owningReverseRelationshipOwningClassPackage;
    private String owningReverseRelationshipOwningClass;
    private String owningReverseRelationshipName;
    private RelationshipAttribute owningRelationship;

    public Attribute(AttributePureType wrapped, MithraObjectTypeWrapper owner)
            throws JavaTypeException
    {
        super(wrapped, owner);
    }

    public Attribute(MithraObjectTypeWrapper owner)
    {
        super(owner);
    }

    public Attribute(MithraObjectTypeWrapper owner, AttributePureType wrapped)
    {
        super(owner, wrapped);
    }

    public String getOwningRelationshipName()
    {
        return this.owningRelationshipName;
    }

    public void setOwningRelationshipName(String owningRelationshipName)
    {
        this.owningRelationshipName = owningRelationshipName;
    }

    public String getOwningReverseRelationshipOwningClassPackage()
    {
        return this.owningReverseRelationshipOwningClassPackage;
    }

    public String getOwningReverseRelationshipOwningClass()
    {
        return this.owningReverseRelationshipOwningClass;
    }

    public String getOwningReverseRelationshipName()
    {
        return this.owningReverseRelationshipName;
    }

    public void setOwningReverseRelationshipName(
            String owningReverseRelationshipOwningClassPackage,
            String owningReverseRelationshipOwningClass,
            String owningReverseRelationshipName)
    {
        this.owningReverseRelationshipOwningClassPackage = owningReverseRelationshipOwningClassPackage;
        this.owningReverseRelationshipOwningClass = owningReverseRelationshipOwningClass;
        this.owningReverseRelationshipName = owningReverseRelationshipName;
    }

    public boolean setOwningRelationship(RelationshipAttribute owningRelationship)
    {
        if (this.owningRelationship == null)
        {
            this.owningRelationship = owningRelationship;
            return true;
        }
        if (owningRelationship.isBetterForAttributeOwnership(this.owningRelationship, this))
        {
            this.owningRelationshipName = null;
            this.owningReverseRelationshipName = null;
            this.owningReverseRelationshipOwningClass = null;
            this.owningReverseRelationshipOwningClassPackage = null;
            return true;
        }
        return false;
    }

    public RelationshipAttribute getOwningRelationship()
    {
        return owningRelationship;
    }
}

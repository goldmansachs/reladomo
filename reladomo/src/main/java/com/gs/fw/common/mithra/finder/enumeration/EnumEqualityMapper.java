
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

package com.gs.fw.common.mithra.finder.enumeration;

import com.gs.fw.common.mithra.attribute.EnumAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.Map;

public class EnumEqualityMapper<E extends Enum<E>> extends EqualityMapper
{
    
    public EnumEqualityMapper(EnumAttribute left, EnumAttribute right)
    {
        super(left, right);
        //todo: make sure reverse mapper is set
    }

    public EnumEqualityMapper(EnumAttribute left, EnumAttribute right, boolean anonymous)
    {
        this(left, right);
        //todo: make sure reverse mapper is set
        this.setAnonymous(anonymous);
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        EnumAttribute right = (EnumAttribute) getRight();
        return right.eq(right.enumValueOf(result));
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        return ((EnumAttribute)getRight()).eq(((EnumAttribute)getLeft()).enumValueOf(original));
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected Mapper substituteNewLeft(Attribute newLeft)
    {
        throw new RuntimeException("not implemented");
    }

}

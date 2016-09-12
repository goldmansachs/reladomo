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

package com.gs.fw.common.mithra.finder.asofop;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.timestamp.TimestampAsOfEqualityMapper;

import java.util.List;



public class AsOfEqualityMapper extends EqualityMapper
{

    public AsOfEqualityMapper(AsOfAttribute left, AsOfAttribute right)
    {
        super(left, right);
        this.setReverseMapper(new AsOfEqualityMapper(right, left, this));
    }

    public AsOfEqualityMapper(AsOfAttribute left, AsOfAttribute right, boolean anonymous)
    {
        this(left, right);
        this.setAnonymous(anonymous);
    }

    public AsOfEqualityMapper(AsOfAttribute left, AsOfAttribute right, AsOfEqualityMapper reverseMapper)
    {
        super(left, right);
        this.setReverseMapper(reverseMapper);
    }

    @Override
    public List map(List joinedList)
    {
        //todo: rezaem: implement in memory finders for dated objects
        throw new RuntimeException("in memory looks ups not yet supported for dated objects");
    }

    @Override
    protected List basicMapOne(Attribute right, Object joined, Operation extraLeftOperation)
    {
        Operation operation = ((AsOfAttribute)this.getLeft()).eq(((AsOfAttribute)right).timestampValueOf(joined));
        if (extraLeftOperation != null) operation = operation.and(extraLeftOperation);
        return operation.getResultObjectPortal().zFindInMemoryWithoutAnalysis(operation, true);
    }


    @Override
    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        //todo: rezaem: implement in memory finders for dated objects
        return null;
    }

    @Override
    protected List basicMap(Attribute right, List joinedList)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected List basicMapReturnNullIfIncompleteIndexHit(Attribute right, List joinedList)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected List basicMapReturnNullIfIncompleteIndexHit(Attribute right, List joinedList, Operation extraOperationOnResult)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void generateSql(SqlQuery query)
    {
        boolean needToGenerate = !(query.isMappedAlready(this));
        query.pushMapper(this);
        if (needToGenerate)
        {
            query.addAsOfAttributeSql();
        }
    }

    @Override
    protected void registerRightAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        super.registerRightAsOfAttributesAndOperations(checker);
        checker.setEqualityAsOfOperation((AsOfAttribute) this.getLeft(), (AsOfAttribute) this.getRight());
    }

    @Override
    protected Mapper substituteNewLeft(Attribute newLeft)
    {
        return new TimestampAsOfEqualityMapper((TimestampAttribute)newLeft, (AsOfAttribute) this.getRight());
    }
}

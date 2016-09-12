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

package com.gs.fw.common.mithra.attribute.calculator;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.CalculatedIntegerAttribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.io.Serializable;
import java.util.Set;
import java.math.BigDecimal;


public interface NumericAttributeCalculator extends Serializable, UpdateCountHolder
{

    public boolean isAttributeNull(Object o);

    public MithraObjectPortal getOwnerPortal();

    public MithraObjectPortal getTopLevelPortal();

    public String getFullyQualifiedCalculatedExpression(SqlQuery query);

    public void addDepenedentAttributesToSet(Set set);

    public AsOfAttribute[] getAsOfAttributes();

    public void generateMapperSql(AggregateSqlQuery query);

    public void forEach(DoubleProcedure proc, Object o, Object context);
    public void forEach(FloatProcedure proc, Object o, Object context);
    public void forEach(LongProcedure proc, Object o, Object context);
    public void forEach(IntegerProcedure proc, Object o, Object context);
    public void forEach(BigDecimalProcedure proc, Object o, Object context);

    public double doubleValueOf(Object o);
    public float floatValueOf(Object o);
    public long longValueOf(Object o);
    public int intValueOf(Object o);
    public BigDecimal bigDecimalValueOf(Object o);

    public void addDependentPortalsToSet(Set set);

    public Operation createMappedOperation();

    public String getTopOwnerClassName();
    public int getScale();
    public int getPrecision();

    public void appendToString(ToStringContext toStringContext);

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext);
    
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute);
}


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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.finder.attribute.ByteAttribute;
import com.gs.fw.finder.attribute.DoubleAttribute;
import com.gs.fw.finder.attribute.FloatAttribute;
import com.gs.fw.finder.attribute.IntegerAttribute;
import com.gs.fw.finder.attribute.LongAttribute;
import com.gs.fw.finder.attribute.ShortAttribute;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;
import java.util.Set;

public interface NumericAttribute<Owner, V> extends com.gs.fw.finder.attribute.NumericAttribute<Owner>, UpdateCountHolder, Function<Owner, V>
{

    public com.gs.fw.common.mithra.MithraAggregateAttribute count();

    public com.gs.fw.common.mithra.MithraAggregateAttribute min();

    public MithraAggregateAttribute max();

    public MithraAggregateAttribute sum();

    public com.gs.fw.common.mithra.MithraAggregateAttribute avg();

    public DoubleAggregateAttribute standardDeviationSample();

    public DoubleAggregateAttribute standardDeviationPopulation();

    public DoubleAggregateAttribute varianceSample();

    public DoubleAggregateAttribute variancePopulation();

    public MithraObjectPortal getOwnerPortal();

    public MithraObjectPortal getTopLevelPortal();

    public String zGetTopOwnerClassName();

    public String getFullyQualifiedLeftHandExpression(SqlQuery query);

    public V valueOf(Owner obj);

    public void generateMapperSql(AggregateSqlQuery query);

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt) throws SQLException;

    public void forEach(IntegerProcedure proc, Owner obj, Object context);

    public void forEach(LongProcedure proc, Owner obj, Object context);

    public void forEach(FloatProcedure proc, Owner obj, Object context);

    public void forEach(DoubleProcedure proc, Owner obj, Object context);

    public void forEach(BigDecimalProcedure proc, Owner obj, Object context);

    public NumericType getNumericType();

    public NumericType getCalculatedType(NumericAttribute other);

    public NumericAttribute getMappedAttributeWithCommonMapper(NumericAttribute calculatedAttribute, Mapper commonMapper, Mapper mapperRemainder, Function parentSelector);

    public NumericAttribute plus(ByteAttribute addend);
    public NumericAttribute plus(ShortAttribute addend);
    public NumericAttribute plus(IntegerAttribute addend);
    public NumericAttribute plus(LongAttribute addend);
    public NumericAttribute plus(FloatAttribute addend);
    public NumericAttribute plus(DoubleAttribute addend);
    public NumericAttribute plus(BigDecimalAttribute addend);

    public NumericAttribute minus(ByteAttribute subtrahend);
    public NumericAttribute minus(ShortAttribute subtrahend);
    public NumericAttribute minus(IntegerAttribute subtrahend);
    public NumericAttribute minus(LongAttribute subtrahend);
    public NumericAttribute minus(FloatAttribute subtrahend);
    public NumericAttribute minus(DoubleAttribute subtrahend);
    public NumericAttribute minus(BigDecimalAttribute subtrahend);

    public NumericAttribute times(ByteAttribute multiplicand);
    public NumericAttribute times(ShortAttribute multiplicand);
    public NumericAttribute times(IntegerAttribute multiplicand);
    public NumericAttribute times(LongAttribute multiplicand);
    public NumericAttribute times(FloatAttribute multiplicand);
    public NumericAttribute times(DoubleAttribute multiplicand);
    public NumericAttribute times(BigDecimalAttribute multiplicand);

    public NumericAttribute dividedBy(ByteAttribute divisor);
    public NumericAttribute dividedBy(ShortAttribute divisor);
    public NumericAttribute dividedBy(IntegerAttribute divisor);
    public NumericAttribute dividedBy(LongAttribute divisor);
    public NumericAttribute dividedBy(FloatAttribute divisor);
    public NumericAttribute dividedBy(DoubleAttribute divisor);
    public NumericAttribute dividedBy(BigDecimalAttribute divisor);

    public NumericAttribute zDispatchAddTo(NumericAttribute firstAddend);
    public NumericAttribute zDispatchSubtractFrom(NumericAttribute minuend);
    public NumericAttribute zDispatchMultiplyBy(NumericAttribute firstMultiplicand);
    public NumericAttribute zDispatchDivideInto(NumericAttribute dividend);

    public void zAddDependentPortalsToSet(Set set);
    public void zAddDepenedentAttributesToSet(Set set);
    public AsOfAttribute[] getAsOfAttributes();
    public Operation zCreateMappedOperation();

    public int getScale();
    public int getPrecision();

    boolean isAttributeNull(Owner o);
}

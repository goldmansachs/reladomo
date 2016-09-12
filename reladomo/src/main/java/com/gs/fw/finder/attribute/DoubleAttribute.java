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

package com.gs.fw.finder.attribute;

import com.gs.collections.api.set.primitive.DoubleSet;
import com.gs.fw.finder.Operation;


public interface DoubleAttribute<Owner> extends NumericAttribute<Owner>
{
    public Operation<Owner> eq(double value);

    public Operation<Owner> notEq(double value);

    public Operation<Owner> greaterThan(double value);

    public Operation<Owner> greaterThanEquals(double value);

    public Operation<Owner> lessThan(double value);

    public Operation<Owner> lessThanEquals(double value);

    public Operation<Owner> in(DoubleSet doubleSet);

    public Operation<Owner> notIn(DoubleSet doubleSet);

    public DoubleAttribute<Owner> plus(ByteAttribute addend);
    public DoubleAttribute<Owner> plus(ShortAttribute addend);
    public DoubleAttribute<Owner> plus(IntegerAttribute addend);
    public DoubleAttribute<Owner> plus(LongAttribute addend);
    public DoubleAttribute<Owner> plus(FloatAttribute addend);
    public DoubleAttribute<Owner> plus(DoubleAttribute addend);

    public DoubleAttribute<Owner> minus(ByteAttribute subtrahend);
    public DoubleAttribute<Owner> minus(ShortAttribute subtrahend);
    public DoubleAttribute<Owner> minus(IntegerAttribute subtrahend);
    public DoubleAttribute<Owner> minus(LongAttribute subtrahend);
    public DoubleAttribute<Owner> minus(FloatAttribute subtrahend);
    public DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    public DoubleAttribute<Owner> times(ByteAttribute multiplicand);
    public DoubleAttribute<Owner> times(ShortAttribute multiplicand);
    public DoubleAttribute<Owner> times(IntegerAttribute multiplicand);
    public DoubleAttribute<Owner> times(LongAttribute multiplicand);
    public DoubleAttribute<Owner> times(FloatAttribute multiplicand);
    public DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    public DoubleAttribute<Owner> dividedBy(ByteAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(ShortAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(IntegerAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(LongAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(FloatAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    public DoubleAttribute<Owner> absoluteValue();
}

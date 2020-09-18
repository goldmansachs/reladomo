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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.finder.attribute;

import com.gs.fw.finder.Operation;
import org.eclipse.collections.api.set.primitive.DoubleSet;


public interface DoubleAttribute<Owner> extends NumericAttribute<Owner>
{
    Operation<Owner> eq(double value);

    Operation<Owner> notEq(double value);

    Operation<Owner> greaterThan(double value);

    Operation<Owner> greaterThanEquals(double value);

    Operation<Owner> lessThan(double value);

    Operation<Owner> lessThanEquals(double value);

    Operation<Owner> in(DoubleSet doubleSet);

    Operation<Owner> notIn(DoubleSet doubleSet);

    DoubleAttribute<Owner> plus(ByteAttribute addend);
    DoubleAttribute<Owner> plus(ShortAttribute addend);
    DoubleAttribute<Owner> plus(IntegerAttribute addend);
    DoubleAttribute<Owner> plus(LongAttribute addend);
    DoubleAttribute<Owner> plus(FloatAttribute addend);
    DoubleAttribute<Owner> plus(DoubleAttribute addend);

    DoubleAttribute<Owner> minus(ByteAttribute subtrahend);
    DoubleAttribute<Owner> minus(ShortAttribute subtrahend);
    DoubleAttribute<Owner> minus(IntegerAttribute subtrahend);
    DoubleAttribute<Owner> minus(LongAttribute subtrahend);
    DoubleAttribute<Owner> minus(FloatAttribute subtrahend);
    DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    DoubleAttribute<Owner> times(ByteAttribute multiplicand);
    DoubleAttribute<Owner> times(ShortAttribute multiplicand);
    DoubleAttribute<Owner> times(IntegerAttribute multiplicand);
    DoubleAttribute<Owner> times(LongAttribute multiplicand);
    DoubleAttribute<Owner> times(FloatAttribute multiplicand);
    DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    DoubleAttribute<Owner> dividedBy(ByteAttribute divisor);
    DoubleAttribute<Owner> dividedBy(ShortAttribute divisor);
    DoubleAttribute<Owner> dividedBy(IntegerAttribute divisor);
    DoubleAttribute<Owner> dividedBy(LongAttribute divisor);
    DoubleAttribute<Owner> dividedBy(FloatAttribute divisor);
    DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    DoubleAttribute<Owner> absoluteValue();
}

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
import org.eclipse.collections.api.set.primitive.IntSet;


public interface IntegerAttribute<Owner> extends NumericAttribute<Owner>
{
    Operation<Owner> eq(int value);

    Operation<Owner> notEq(int value);

    Operation<Owner> greaterThan(int value);

    Operation<Owner> greaterThanEquals(int value);

    Operation<Owner> lessThan(int value);

    Operation<Owner> lessThanEquals(int value);

    Operation<Owner> in(IntSet intSet);

    Operation<Owner> notIn(IntSet intSet);

    IntegerAttribute<Owner> plus(ByteAttribute addend);
    IntegerAttribute<Owner> plus(ShortAttribute addend);
    IntegerAttribute<Owner> plus(IntegerAttribute addend);
    LongAttribute<Owner> plus(LongAttribute addend);
    FloatAttribute<Owner> plus(FloatAttribute addend);
    DoubleAttribute<Owner> plus(DoubleAttribute addend);

    IntegerAttribute<Owner> minus(ByteAttribute subtrahend);
    IntegerAttribute<Owner> minus(ShortAttribute subtrahend);
    IntegerAttribute<Owner> minus(IntegerAttribute subtrahend);
    LongAttribute<Owner> minus(LongAttribute subtrahend);
    FloatAttribute<Owner> minus(FloatAttribute subtrahend);
    DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    IntegerAttribute<Owner> times(ByteAttribute multiplicand);
    IntegerAttribute<Owner> times(ShortAttribute multiplicand);
    IntegerAttribute<Owner> times(IntegerAttribute multiplicand);
    LongAttribute<Owner> times(LongAttribute multiplicand);
    FloatAttribute<Owner> times(FloatAttribute multiplicand);
    DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    IntegerAttribute<Owner> dividedBy(ByteAttribute divisor);
    IntegerAttribute<Owner> dividedBy(ShortAttribute divisor);
    IntegerAttribute<Owner> dividedBy(IntegerAttribute divisor);
    LongAttribute<Owner> dividedBy(LongAttribute divisor);
    FloatAttribute<Owner> dividedBy(FloatAttribute divisor);
    DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    IntegerAttribute<Owner> absoluteValue();
}

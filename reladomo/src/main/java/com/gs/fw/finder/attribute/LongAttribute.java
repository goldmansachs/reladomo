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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

import com.gs.fw.finder.Operation;
import org.eclipse.collections.api.set.primitive.LongSet;


public interface LongAttribute<Owner> extends NumericAttribute<Owner>
{
    Operation<Owner> eq(long value);

    Operation<Owner> notEq(long value);

    Operation<Owner> greaterThan(long value);

    Operation<Owner> greaterThanEquals(long value);

    Operation<Owner> lessThan(long value);

    Operation<Owner> lessThanEquals(long value);

    Operation<Owner> in(LongSet longSet);

    Operation<Owner> notIn(LongSet longSet);

    LongAttribute<Owner> plus(ByteAttribute addend);
    LongAttribute<Owner> plus(ShortAttribute addend);
    LongAttribute<Owner> plus(IntegerAttribute addend);
    LongAttribute<Owner> plus(LongAttribute addend);
    FloatAttribute<Owner> plus(FloatAttribute addend);
    DoubleAttribute<Owner> plus(DoubleAttribute addend);

    LongAttribute<Owner> minus(ByteAttribute subtrahend);
    LongAttribute<Owner> minus(ShortAttribute subtrahend);
    LongAttribute<Owner> minus(IntegerAttribute subtrahend);
    LongAttribute<Owner> minus(LongAttribute subtrahend);
    FloatAttribute<Owner> minus(FloatAttribute subtrahend);
    DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    LongAttribute<Owner> times(ByteAttribute multiplicand);
    LongAttribute<Owner> times(ShortAttribute multiplicand);
    LongAttribute<Owner> times(IntegerAttribute multiplicand);
    LongAttribute<Owner> times(LongAttribute multiplicand);
    FloatAttribute<Owner> times(FloatAttribute multiplicand);
    DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    LongAttribute<Owner> dividedBy(ByteAttribute divisor);
    LongAttribute<Owner> dividedBy(ShortAttribute divisor);
    LongAttribute<Owner> dividedBy(IntegerAttribute divisor);
    LongAttribute<Owner> dividedBy(LongAttribute divisor);
    FloatAttribute<Owner> dividedBy(FloatAttribute divisor);
    DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    LongAttribute<Owner> absoluteValue();
}

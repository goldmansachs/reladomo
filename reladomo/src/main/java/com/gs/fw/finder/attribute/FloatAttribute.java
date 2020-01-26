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
import org.eclipse.collections.api.set.primitive.FloatSet;


public interface FloatAttribute<Owner> extends NumericAttribute<Owner>
{
    Operation<Owner> eq(float value);

    Operation<Owner> notEq(float value);

    Operation<Owner> greaterThan(float value);

    Operation<Owner> greaterThanEquals(float value);

    Operation<Owner> lessThan(float value);

    Operation<Owner> lessThanEquals(float value);

    Operation<Owner> in(FloatSet floatSet);

    Operation<Owner> notIn(FloatSet floatSet);

    FloatAttribute<Owner> plus(ByteAttribute addend);
    FloatAttribute<Owner> plus(ShortAttribute addend);
    FloatAttribute<Owner> plus(IntegerAttribute addend);
    FloatAttribute<Owner> plus(LongAttribute addend);
    FloatAttribute<Owner> plus(FloatAttribute addend);
    DoubleAttribute<Owner> plus(DoubleAttribute addend);

    FloatAttribute<Owner> minus(ByteAttribute subtrahend);
    FloatAttribute<Owner> minus(ShortAttribute subtrahend);
    FloatAttribute<Owner> minus(IntegerAttribute subtrahend);
    FloatAttribute<Owner> minus(LongAttribute subtrahend);
    FloatAttribute<Owner> minus(FloatAttribute subtrahend);
    DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    FloatAttribute<Owner> times(ByteAttribute multiplicand);
    FloatAttribute<Owner> times(ShortAttribute multiplicand);
    FloatAttribute<Owner> times(IntegerAttribute multiplicand);
    FloatAttribute<Owner> times(LongAttribute multiplicand);
    FloatAttribute<Owner> times(FloatAttribute multiplicand);
    DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    FloatAttribute<Owner> dividedBy(ByteAttribute divisor);
    FloatAttribute<Owner> dividedBy(ShortAttribute divisor);
    FloatAttribute<Owner> dividedBy(IntegerAttribute divisor);
    FloatAttribute<Owner> dividedBy(LongAttribute divisor);
    FloatAttribute<Owner> dividedBy(FloatAttribute divisor);
    DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    FloatAttribute<Owner> absoluteValue();
}

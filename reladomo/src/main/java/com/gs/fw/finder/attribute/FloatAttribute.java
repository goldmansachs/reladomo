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
    public Operation<Owner> eq(float value);

    public Operation<Owner> notEq(float value);

    public Operation<Owner> greaterThan(float value);

    public Operation<Owner> greaterThanEquals(float value);

    public Operation<Owner> lessThan(float value);

    public Operation<Owner> lessThanEquals(float value);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> in(com.gs.collections.api.set.primitive.FloatSet floatSet);

    public Operation<Owner> in(FloatSet floatSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> notIn(com.gs.collections.api.set.primitive.FloatSet floatSet);

    public Operation<Owner> notIn(FloatSet floatSet);

    public FloatAttribute<Owner> plus(ByteAttribute addend);
    public FloatAttribute<Owner> plus(ShortAttribute addend);
    public FloatAttribute<Owner> plus(IntegerAttribute addend);
    public FloatAttribute<Owner> plus(LongAttribute addend);
    public FloatAttribute<Owner> plus(FloatAttribute addend);
    public DoubleAttribute<Owner> plus(DoubleAttribute addend);

    public FloatAttribute<Owner> minus(ByteAttribute subtrahend);
    public FloatAttribute<Owner> minus(ShortAttribute subtrahend);
    public FloatAttribute<Owner> minus(IntegerAttribute subtrahend);
    public FloatAttribute<Owner> minus(LongAttribute subtrahend);
    public FloatAttribute<Owner> minus(FloatAttribute subtrahend);
    public DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    public FloatAttribute<Owner> times(ByteAttribute multiplicand);
    public FloatAttribute<Owner> times(ShortAttribute multiplicand);
    public FloatAttribute<Owner> times(IntegerAttribute multiplicand);
    public FloatAttribute<Owner> times(LongAttribute multiplicand);
    public FloatAttribute<Owner> times(FloatAttribute multiplicand);
    public DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    public FloatAttribute<Owner> dividedBy(ByteAttribute divisor);
    public FloatAttribute<Owner> dividedBy(ShortAttribute divisor);
    public FloatAttribute<Owner> dividedBy(IntegerAttribute divisor);
    public FloatAttribute<Owner> dividedBy(LongAttribute divisor);
    public FloatAttribute<Owner> dividedBy(FloatAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    public FloatAttribute<Owner> absoluteValue();
}

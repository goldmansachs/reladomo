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

import com.gs.collections.api.set.primitive.IntSet;
import com.gs.fw.finder.Operation;


public interface IntegerAttribute<Owner> extends NumericAttribute<Owner>
{
    public Operation<Owner> eq(int value);

    public Operation<Owner> notEq(int value);

    public Operation<Owner> greaterThan(int value);

    public Operation<Owner> greaterThanEquals(int value);

    public Operation<Owner> lessThan(int value);

    public Operation<Owner> lessThanEquals(int value);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> in(IntSet intSet);

    public Operation<Owner> in(org.eclipse.collections.api.set.primitive.IntSet intSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> notIn(IntSet intSet);

    public Operation<Owner> notIn(org.eclipse.collections.api.set.primitive.IntSet intSet);

    public IntegerAttribute<Owner> plus(ByteAttribute addend);
    public IntegerAttribute<Owner> plus(ShortAttribute addend);
    public IntegerAttribute<Owner> plus(IntegerAttribute addend);
    public LongAttribute<Owner> plus(LongAttribute addend);
    public FloatAttribute<Owner> plus(FloatAttribute addend);
    public DoubleAttribute<Owner> plus(DoubleAttribute addend);

    public IntegerAttribute<Owner> minus(ByteAttribute subtrahend);
    public IntegerAttribute<Owner> minus(ShortAttribute subtrahend);
    public IntegerAttribute<Owner> minus(IntegerAttribute subtrahend);
    public LongAttribute<Owner> minus(LongAttribute subtrahend);
    public FloatAttribute<Owner> minus(FloatAttribute subtrahend);
    public DoubleAttribute<Owner> minus(DoubleAttribute subtrahend);

    public IntegerAttribute<Owner> times(ByteAttribute multiplicand);
    public IntegerAttribute<Owner> times(ShortAttribute multiplicand);
    public IntegerAttribute<Owner> times(IntegerAttribute multiplicand);
    public LongAttribute<Owner> times(LongAttribute multiplicand);
    public FloatAttribute<Owner> times(FloatAttribute multiplicand);
    public DoubleAttribute<Owner> times(DoubleAttribute multiplicand);

    public IntegerAttribute<Owner> dividedBy(ByteAttribute divisor);
    public IntegerAttribute<Owner> dividedBy(ShortAttribute divisor);
    public IntegerAttribute<Owner> dividedBy(IntegerAttribute divisor);
    public LongAttribute<Owner> dividedBy(LongAttribute divisor);
    public FloatAttribute<Owner> dividedBy(FloatAttribute divisor);
    public DoubleAttribute<Owner> dividedBy(DoubleAttribute divisor);

    public IntegerAttribute<Owner> absoluteValue();
}

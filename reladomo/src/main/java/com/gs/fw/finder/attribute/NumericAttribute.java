
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

import com.gs.fw.finder.Attribute;
import com.gs.fw.finder.AggregateAttribute;

public interface NumericAttribute<Owner> extends Attribute<Owner>
{
    public NumericAttribute<Owner> plus(ByteAttribute addend);
    public NumericAttribute<Owner> plus(ShortAttribute addend);
    public NumericAttribute<Owner> plus(IntegerAttribute addend);
    public NumericAttribute<Owner> plus(LongAttribute addend);
    public NumericAttribute<Owner> plus(FloatAttribute addend);
    public NumericAttribute<Owner> plus(DoubleAttribute addend);

    public NumericAttribute<Owner> minus(ByteAttribute subtrahend);
    public NumericAttribute<Owner> minus(ShortAttribute subtrahend);
    public NumericAttribute<Owner> minus(IntegerAttribute subtrahend);
    public NumericAttribute<Owner> minus(LongAttribute subtrahend);
    public NumericAttribute<Owner> minus(FloatAttribute subtrahend);
    public NumericAttribute<Owner> minus(DoubleAttribute subtrahend);

    public NumericAttribute<Owner> times(ByteAttribute multiplicand);
    public NumericAttribute<Owner> times(ShortAttribute multiplicand);
    public NumericAttribute<Owner> times(IntegerAttribute multiplicand);
    public NumericAttribute<Owner> times(LongAttribute multiplicand);
    public NumericAttribute<Owner> times(FloatAttribute multiplicand);
    public NumericAttribute<Owner> times(DoubleAttribute multiplicand);

    public NumericAttribute<Owner> dividedBy(ByteAttribute divisor);
    public NumericAttribute<Owner> dividedBy(ShortAttribute divisor);
    public NumericAttribute<Owner> dividedBy(IntegerAttribute divisor);
    public NumericAttribute<Owner> dividedBy(LongAttribute divisor);
    public NumericAttribute<Owner> dividedBy(FloatAttribute divisor);
    public NumericAttribute<Owner> dividedBy(DoubleAttribute divisor);

    public NumericAttribute<Owner> absoluteValue();

    public AggregateAttribute<Owner> sum();

    public AggregateAttribute<Owner> avg();

    public AggregateAttribute<Owner> standardDeviationSample();
}

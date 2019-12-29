
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
    NumericAttribute<Owner> plus(ByteAttribute addend);
    NumericAttribute<Owner> plus(ShortAttribute addend);
    NumericAttribute<Owner> plus(IntegerAttribute addend);
    NumericAttribute<Owner> plus(LongAttribute addend);
    NumericAttribute<Owner> plus(FloatAttribute addend);
    NumericAttribute<Owner> plus(DoubleAttribute addend);

    NumericAttribute<Owner> minus(ByteAttribute subtrahend);
    NumericAttribute<Owner> minus(ShortAttribute subtrahend);
    NumericAttribute<Owner> minus(IntegerAttribute subtrahend);
    NumericAttribute<Owner> minus(LongAttribute subtrahend);
    NumericAttribute<Owner> minus(FloatAttribute subtrahend);
    NumericAttribute<Owner> minus(DoubleAttribute subtrahend);

    NumericAttribute<Owner> times(ByteAttribute multiplicand);
    NumericAttribute<Owner> times(ShortAttribute multiplicand);
    NumericAttribute<Owner> times(IntegerAttribute multiplicand);
    NumericAttribute<Owner> times(LongAttribute multiplicand);
    NumericAttribute<Owner> times(FloatAttribute multiplicand);
    NumericAttribute<Owner> times(DoubleAttribute multiplicand);

    NumericAttribute<Owner> dividedBy(ByteAttribute divisor);
    NumericAttribute<Owner> dividedBy(ShortAttribute divisor);
    NumericAttribute<Owner> dividedBy(IntegerAttribute divisor);
    NumericAttribute<Owner> dividedBy(LongAttribute divisor);
    NumericAttribute<Owner> dividedBy(FloatAttribute divisor);
    NumericAttribute<Owner> dividedBy(DoubleAttribute divisor);

    NumericAttribute<Owner> absoluteValue();

    AggregateAttribute<Owner> sum();

    AggregateAttribute<Owner> avg();

    AggregateAttribute<Owner> standardDeviationSample();
}

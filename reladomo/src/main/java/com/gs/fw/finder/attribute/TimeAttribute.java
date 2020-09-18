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

import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.finder.Attribute;
import com.gs.fw.finder.Operation;

import java.util.Set;

public interface TimeAttribute<Owner> extends Attribute<Owner>
{
    Operation<Owner> eq(Time value);

    Operation<Owner> notEq(Time value);

    Operation<Owner> greaterThan(Time value);

    Operation<Owner> greaterThanEquals(Time value);

    Operation<Owner> lessThan(Time value);

    Operation<Owner> lessThanEquals(Time value);

    Operation<Owner> in(Set<Time> timeSet);

    Operation<Owner> notIn(Set<Time> timeSet);
}

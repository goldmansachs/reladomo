
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
import com.gs.fw.finder.Operation;

import java.util.Set;

public interface StringAttribute<Owner> extends Attribute<Owner>
{
    Operation<Owner> eq(String value);

    Operation<Owner> notEq(String value);

    Operation<Owner> in(Set<String> stringSet);

    Operation<Owner> notIn(Set<String> stringSet);

    Operation<Owner> greaterThan(String value);

    Operation<Owner> greaterThanEquals(String value);

    Operation<Owner> lessThan(String value);

    Operation<Owner> lessThanEquals(String value);

    Operation<Owner> startsWith(String value);

    Operation<Owner> notStartsWith(String value);

    Operation<Owner> endsWith(String value);

    Operation<Owner> notEndsWith(String value);

    Operation<Owner> contains(String value);

    Operation<Owner> notContains(String value);

    StringAttribute<Owner> toLowerCase();
}

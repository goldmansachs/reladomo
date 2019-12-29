
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
    public Operation<Owner> eq(String value);

    public Operation<Owner> notEq(String value);

    public Operation<Owner> in(Set<String> stringSet);

    public Operation<Owner> notIn(Set<String> stringSet);

    public Operation<Owner> greaterThan(String value);

    public Operation<Owner> greaterThanEquals(String value);

    public Operation<Owner> lessThan(String value);

    public Operation<Owner> lessThanEquals(String value);

    public Operation<Owner> startsWith(String value);

    public Operation<Owner> notStartsWith(String value);

    public Operation<Owner> endsWith(String value);

    public Operation<Owner> notEndsWith(String value);

    public Operation<Owner> contains(String value);

    public Operation<Owner> notContains(String value);

    public StringAttribute<Owner> toLowerCase();
}

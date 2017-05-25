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

import com.gs.collections.api.set.primitive.CharSet;
import com.gs.fw.finder.Attribute;
import com.gs.fw.finder.Operation;


public interface CharacterAttribute<Owner> extends Attribute<Owner>
{
    public Operation<Owner> eq(char value);

    public Operation<Owner> notEq(char value);

    public Operation<Owner> greaterThan(char value);

    public Operation<Owner> greaterThanEquals(char value);

    public Operation<Owner> lessThan(char value);

    public Operation<Owner> lessThanEquals(char value);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> in(CharSet charSet);

    public Operation<Owner> in(org.eclipse.collections.api.set.primitive.CharSet charSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public Operation<Owner> notIn(CharSet charSet);

    public Operation<Owner> notIn(org.eclipse.collections.api.set.primitive.CharSet charSet);
}

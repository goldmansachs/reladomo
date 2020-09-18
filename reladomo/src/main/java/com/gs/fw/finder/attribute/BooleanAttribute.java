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

import com.gs.fw.finder.Attribute;
import com.gs.fw.finder.Operation;
import org.eclipse.collections.api.set.primitive.BooleanSet;


public interface BooleanAttribute<Owner> extends Attribute<Owner>
{
    Operation<Owner> eq(boolean value);

    Operation<Owner> notEq(boolean value);

    Operation<Owner> in(BooleanSet booleanSet);

    Operation<Owner> notIn(BooleanSet booleanSet);
}

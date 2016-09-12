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

package com.gs.fw.common.mithra.generator.queryparser;

import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;

/**
 * Created by IntelliJ IDEA.
 * User: adewuad
 * Date: Nov 17, 2004
 * Time: 12:43:00 PM
 * To change this template use File | Settings | File Templates.
 */
public interface LeafLevelExpression
{
    public boolean involvesThis();

    public boolean involvesClassAsNonThis(MithraObjectTypeWrapper owner);

    public boolean involvesOnlyThis(MithraObjectTypeWrapper mithraObjectTypeWrapper);
}


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

package com.gs.fw.common.mithra.extractor;

public abstract class AbstractEnumExtractor<T, E extends Enum<E>> extends NonPrimitiveExtractor<T, E> implements EnumExtractor<T, E>
{

    public E valueOf(T o)
    {
        return this.enumValueOf(o);
    }

    public void setValue(T o, E newValue)
    {
        this.setEnumValue(o, newValue);
    }
}

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

package com.gs.fw.common.mithra.generator.computedattribute.type;


public class NumberType implements Type
{
    @Override
    public boolean isCompatibleWith(Type type)
    {
        return type == null || type instanceof NumberType;
    }

    public Object convertFromDouble(double n)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Type computeMostCompatibleType(Type other)
    {
        return this; //todo: resolve number types more specifically during parsing.
    }
}

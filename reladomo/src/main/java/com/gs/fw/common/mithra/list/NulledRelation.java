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

package com.gs.fw.common.mithra.list;


public class NulledRelation
{
    private static final NulledRelation instance = new NulledRelation(null);

    private final Object original;


    public static NulledRelation getInstance()
    {
        return instance;
    }

    public static NulledRelation create(Object original)
    {
        if (original == null) return instance;
        return new NulledRelation(original);
    }

    private NulledRelation(Object original)
    {
        this.original = original;
    }

    public Object getOriginal()
    {
        return original;
    }
}

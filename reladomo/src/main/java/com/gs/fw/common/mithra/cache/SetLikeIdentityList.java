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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure2;
import com.gs.fw.common.mithra.util.DoUntilProcedure3;

import java.util.List;


public interface SetLikeIdentityList<T>
{

    public SetLikeIdentityList<T> addAndGrow(T toAdd);

    public Object removeAndShrink(T toRemove);

    public T getFirst();

    public List<T> getAll();

    public int size();

    public boolean contains(Object o);

    public boolean forAll(DoUntilProcedure procedure);

    public boolean forAllWith(DoUntilProcedure2<T, Object> procedure, Object param);

    public boolean forAllWith(DoUntilProcedure3<T, Object, Object> procedure, Object param1, Object param2);
}

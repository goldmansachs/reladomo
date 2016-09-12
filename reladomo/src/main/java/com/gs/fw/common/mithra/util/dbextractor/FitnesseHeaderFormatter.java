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

package com.gs.fw.common.mithra.util.dbextractor;

import com.gs.collections.api.block.function.Function;
import com.gs.fw.common.mithra.finder.RelatedFinder;


public class FitnesseHeaderFormatter implements Function<Class, String>
{
    private static final String QUOTES = "'''";

    public String valueOf(Class finderClass)
    {
        String className = finderClass.getSimpleName();
        if (RelatedFinder.class.isAssignableFrom(finderClass))
        {
            return QUOTES + className.substring(0, className.indexOf("SingleFinder")) + QUOTES;
        }

        throw new RuntimeException("MithraTestDataHeaderFormatter received a non RelatedFinder class : " + className);
    }
}

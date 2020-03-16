/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.reladomo.graphql;

public class StringKit
{
    protected static String capitalize (String str)
    {
        if (str == null || str.length () == 0) return str;
        return Character.toUpperCase (str.charAt (0)) + str.substring (1);
    }

    protected static String decapitalize (String str, int skip)
    {
        str = str.substring (skip);
        if (str == null || str.length () == 0) return str;
        return Character.toLowerCase (str.charAt (0)) + str.substring (1);
    }

    // todo: combine with StringUtility in reladomogen, consider using https://github.com/plural4j/plural4j
    public static String englishPluralize(String tmp)
    {
        char last = tmp.charAt(tmp.length() - 1);
        if (last == 's' || last == 'x')
        {
            return tmp + "es";
        }
        if (last == 'y') return tmp.substring(0, tmp.length() - 1)+"ies";
        return tmp + 's';
    }
}

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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;

import java.util.Map;

class OrderByBuilder
{

    OrderBy build (Map<String, String> val, RelatedFinder finder)
    {
        if (val.size () != 1)
            throw new RuntimeException ("cannot define OrderBy for " + val + " on " + finder.getClass ().getName ());
        String attributeName = val.keySet ().iterator ().next ();
        String order = val.get(attributeName);


        Attribute attr = finder.getAttributeByName (attributeName);
        return "asc".equals (order) ? attr.ascendingOrderBy () : attr.descendingOrderBy ();
    }
}

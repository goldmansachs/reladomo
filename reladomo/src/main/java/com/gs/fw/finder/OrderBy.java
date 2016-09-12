
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

package com.gs.fw.finder;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Orderings for {@link DomainList}s of objects returned from queries.
 */
public interface OrderBy<Owner> extends Comparator<Owner>, Serializable
{
    /**
     * Combines this order by criteria with the given order by criteria resulting in a hierarchical ordering.
     *
     * @param other The order by criteria to combine.
     * @return the combined order by criteria.
     */
    public OrderBy<Owner> and(OrderBy<Owner> other);
}

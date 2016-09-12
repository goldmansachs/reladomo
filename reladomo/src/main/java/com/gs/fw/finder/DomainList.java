
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
import java.util.List;
import java.util.RandomAccess;

/**
 * Collections of domain objects.
 */
public interface DomainList<E> extends List<E>, RandomAccess, Serializable
{
    /**
     * Counts the number of elements in the list. Compared with {@link java.util.List#size()}, this optimizes scenarios
     * in which the caller is more interested in the number of elements in the list than in performing operations on the
     * contained elements.
     *
     * @return the number of elements in the list.
     */
    public int count();

    /**
     * Specifies the criteria for the ordering of retrieved objects. Orderings are hierarchical, so they will be applied
     * in the order they appear. This overwrites the result of previous calls to <tt>setOrderBy</tt> and
     * <tt>addOrderBy</tt>.
     *
     * @param orderBy The order by criteria.
     */
    public void setOrderBy(OrderBy<E> orderBy);

    /**
     * Limits the number of objects to retrieve. It limits the amount of IO that will be performed.
     * The returned list may have a size greater than the limit. The size may be greater if
     * the list is resolved without IO.
     *
     * @param limit The limit on the number of objects to retrieve.
     */
    public void setMaxObjectsToRetrieve(int limit);

    /**
     * Provides a hint to optimize retrieval of the indicated associations. This may be called multiple times to
     * optimize the retrieval of multiple associations. Since this is only a hint, implementations may choose to ignore
     * it.
     * <p/>
     * It is typically possible to bulk fetch associations for a collection of objects more efficiently than fetching
     * them one by one. Therefore, if the caller is certain that an association will be navigated for multiple objects,
     * the associations to pre-fetch should always be specified.
     *
     * @param navigationToOptimize The association to optimize.
     */
    public void deepFetch(Navigation<E> navigationToOptimize);

    /**
     * Provides a hint to restrict retrieval to the indicated attribute. This may be called multiple times to restrict
     * results to multiple attributes. Since this is only a hint, implementations may choose to ignore it.
     * <p/>
     * Generally speaking, retrieving all attributes of a domain object is trivially more expensive than retrieving
     * a subset. Therefore, when retrieval is not explicitly restricted to specific attributes the default is to return
     * them all. In some cases (e.g., where a domain object is composed of data from multiple tables or even multiple
     * data sources) it is possible to optimize query time by restricting results to a subset of attributes.
     * <p/>
     * By restricting retrieval to specific attributes, the caller is signaling interest in only those attributes.
     * Attempts to access other attributes may result in poor performance. As such, this should be used carefully.
     *
     * @param attributeToWhiteList The attribute to which results should be restricted.
     */
    public void restrictRetrievalTo(Attribute<E> attributeToWhiteList);

}

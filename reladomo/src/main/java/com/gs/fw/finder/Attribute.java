
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

/**
 * Domain object attributes for use in generating query criteria. Attributes are factories for {@link Operation}s,
 * {@link OrderBy}s, and synthetic attribute-based constructs such as {@link AggregateAttribute}s.
 */
public interface Attribute<Owner> extends Serializable
{
    /**
     * Produces query criteria specifying that this attribute is null.
     *
     * @return an operation specifying that this attribute is null.
     */
    public Operation<Owner> isNull();

    /**
     * Produces query criteria specifying that this attribute is not null.
     *
     * @return an operation specifying that this attribute is not null.
     */
    public Operation<Owner> isNotNull();

    /**
     * Produces order by criteria representing an ascending ordering on this attribute.
     *
     * @return an order by representing an ascending ordering on this attribute.
     */
    public OrderBy<Owner> ascendingOrderBy();

    /**
     * Produces order by criteria representing a descending ordering on this attribute.
     *
     * @return an order by representing a descendingg ordering on this attribute.
     */
    public OrderBy<Owner> descendingOrderBy();

    /**
     * Produces an aggregate attribute representing a count operation on this attribute.
     *
     * @return an aggregate attribute representing a count operation this attribute.
     */
    public AggregateAttribute<Owner> count();

    /**
     * Produces an aggregate attribute representing a min operation on this attribute.
     *
     * @return an aggregate attribute representing a min operation on this attribute.
     */
    public AggregateAttribute<Owner> min();

    /**
     * Produces an aggregate attribute representing a max operation on this attribute.
     *
     * @return an aggregate attribute representing a max operation on this attribute.
     */
    public AggregateAttribute<Owner> max();
}

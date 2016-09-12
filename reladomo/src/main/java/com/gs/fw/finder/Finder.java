
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

/**
 * Initiators for queries against domain models. Finders use {@link Operation}s as criteria and return results as
 * individual domain objects or {@link DomainList}s of domain objects. 
 */
public interface Finder<Result>
{
    /**
     * Creates an operation that will return all objects.
     *
     * @return criteria to match all objects.
     */
    public Operation all();

    /**
     * Find a single object using the specified criteria.
     * <p/>
     * This is a convenience method for use when it is certain the given criteria matches a single object.
     * <p/>
     * This returns null if no object matches the given criteria. It throws a runtime exception if the given criteria
     * match multiple objects.
     *
     * @param criteria Search criteria specifying a unique object.
     * @return the object that matches the given criteria or null.
     */
    public Result findOne(Operation<Result> criteria);

    /**
     * Finds all objects using the specified criteria.
     * <p/>
     * If no objects match the given criteria, this returns an empty list.
     *
     * @param criteria The search criteria.
     * @return a list of objects matching the given criteria or an empty list.
     */
    public DomainList<? extends Result> findMany(Operation<Result> criteria);
}

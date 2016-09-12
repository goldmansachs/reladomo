
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
 * Navigations around the domain model for use in generating query criteria. Associations are factories for
 * {@link Operation}s. Navigation to other domain objects allows use of their attributes and navigations for generating
 * query criteria.
 */
public interface Navigation<Owner> extends Serializable
{
    /**
     * Produces query criteria specifying that this association exists.
     *
     * @return an operation specifying that this association exists.
     */
    public Operation<Owner> exists();

    /**
     * Produces query criteria specifying that this association does not exist.
     *
     * @return an operation specifying that this association does not exist.
     */
    public Operation<Owner> notExists();
}

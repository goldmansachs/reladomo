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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.fw.common.mithra.util.Filter;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


public interface CacheLoaderManager
{
    /**
     * load all classes defined in configuration file for provided business date
     *
     * @param businessDates list of business dates to be loaded. For non-business dated caches, this parameter should be an empty list.
     * @param initialLoadEndTime used as a processing time up to which the data is loaded. The same processing time need to be used as a starting point of the next refresh
     * @param monitor
     */
    public void runInitialLoad(List<Timestamp> businessDates, Timestamp initialLoadEndTime, CacheLoaderMonitor monitor);

    /**
     * load all classes defined in configuration file for provided business date and refresh interval
     *
     * @param businessDate list of business dates to use for refresh. For non-business dated caches, this parameter should be an empty list.
     * @param refreshInterval
     * @param monitor
     */
    public void runRefresh(List<Timestamp> businessDate, RefreshInterval refreshInterval, CacheLoaderMonitor monitor);

    /**
     * load subset of classes defined in the classesToLoadWithAdditionalOperations keys.
     * This can be used to load/reload individual classes as well as load missing classses for corrupt archive.
     *
     * @param businessDate list of business dates to be loaded. For non-business dated caches, this parameter should be an empty list.
     * @param classesToLoadWithAdditionalOperations
     *                      map of classes to load to the additionalOperationBuilders. If additionalOperationBuilder is null, will load all instances defined by the cacheLoader.xml
     * @param loadDependent specifies whether to load classes dependent on the classesToLoad list.
     * @param monitor
     */
    public void runQualifiedLoad(List<Timestamp> businessDate,
                                 Map<String, AdditionalOperationBuilder> classesToLoadWithAdditionalOperations,
                                 boolean loadDependent,
                                 CacheLoaderMonitor monitor);

    Map<String, ? extends Filter> createCacheFilterOfDatesToKeep(List<Timestamp> businessDates);

    /**
     * load dependent classes defined in the ownerObjects.
     *
     * @param ownerObjects
     * @param businessDates list of business dates to be loaded. For non-business dated caches, this parameter should be an empty list.
     * @param loadEndTime
     * @param monitor
     */
    public void loadDependentObjectsFor(List ownerObjects, List<Timestamp> businessDates, Timestamp loadEndTime, CacheLoaderMonitor monitor);
}

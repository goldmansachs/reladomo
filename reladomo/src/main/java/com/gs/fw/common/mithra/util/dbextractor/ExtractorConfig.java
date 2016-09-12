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


/**
 * Provides configuration parameters to MithraObjectGraphExtractor
 *
 * @see com.gs.fw.common.mithra.util.dbextractor.MithraObjectGraphExtractor
 */
public interface ExtractorConfig
{
    /**
     * Size of the thread pool to use for traversing relationships
     *
     * @return thread pool size
     */
    int getThreadPoolSize();

    /**
     * Number of seconds to allow traversing a relationship before timing out and failing the extract.
     *
     * @return the timeout in seconds
     */
    int getTimeoutSeconds();

    /**
     * The maximum ratio allowed between the number of related objects extracted divided by the number of objects from
     * which they were extracted. If this ratio is exceeded the extract fails and additional filtering needs to be added
     * or ratio increased.
     * @return - the maximum extract threshold ratio allowed before failing the extract
     */
    int getExtractThresholdRatio();
}

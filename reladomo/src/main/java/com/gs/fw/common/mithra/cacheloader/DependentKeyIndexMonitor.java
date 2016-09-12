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

/*
 * Application :
 *
 *
 *
 *
 */
package com.gs.fw.common.mithra.cacheloader;

public class DependentKeyIndexMonitor
{
    private String className;
    private String threadPoolName;
    private String extractors;
    private int keyCount;

    public DependentKeyIndexMonitor(String className, String threadPoolName, String extractors, int keyCount)
    {
        this.className = className;
        this.threadPoolName = threadPoolName;
        this.extractors = extractors;
        this.keyCount = keyCount;
    }

    public String getClassName()
    {
        return this.className;
    }

    public String getThreadPoolName()
    {
        return this.threadPoolName;
    }

    public String getExtractors()
    {
        return this.extractors;
    }

    public int getKeyCount()
    {
        return this.keyCount;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(className).append(".").append(extractors).append("@").append(threadPoolName)
                .append(" keyCount: ").append(keyCount);
        return builder.toString();
    }
}

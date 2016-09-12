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

package com.gs.fw.common.mithra.util;


public class PerformanceDataPerOperation
{
    private int totalOperations;
    private int totalObjects;
    private int totalTime;

    public void addTime(int objectsFound, long time)
    {
        totalOperations++;
        totalObjects += objectsFound;
        totalTime += time;
    }

    public int getTotalOperations()
    {
        return totalOperations;
    }

    public int getTotalObjects()
    {
        return totalObjects;
    }

    public int getTotalTime()
    {
        return totalTime;
    }

    public void clear()
    {
        this.totalObjects = 0;
        this.totalOperations = 0;
        this.totalTime = 0;
    }
}

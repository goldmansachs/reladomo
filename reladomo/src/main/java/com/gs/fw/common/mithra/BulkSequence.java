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

package com.gs.fw.common.mithra;


public class BulkSequence
{

    private long start;
    private long exclusiveEnd;
    private long increment;

    public BulkSequence(long start, long exclusiveEnd, long increment)
    {
        this.start = start;
        this.exclusiveEnd = exclusiveEnd;
        this.increment = increment;
    }

    public boolean hasMore()
    {
        if (increment < 0)
        {
            return this.start > this.exclusiveEnd;
        }
        return this.start < this.exclusiveEnd;
    }

    public long getNext()
    {
        long result = start;
        start += increment;
        return result;
    }

}

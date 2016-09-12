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

import java.sql.Timestamp;

public class RefreshInterval
{
    private Timestamp start;
    private Timestamp end;

    public RefreshInterval(Timestamp start, Timestamp end)
    {
        this.start = start;
        this.end = end;
    }

    public Timestamp getStart()
    {
        return start;
    }

    public Timestamp getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "RefreshInterval{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        RefreshInterval that = (RefreshInterval) o;

        if (end != null ? !end.equals(that.end) : that.end != null)
        {
            return false;
        }
        if (start != null ? !start.equals(that.start) : that.start != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = start != null ? start.hashCode() : 0;
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
    }
}

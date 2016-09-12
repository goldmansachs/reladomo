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


public class StatisticCounter
{
    protected int hits;
    protected int total;


    public void registerHit(boolean isHit)
    {
        if (isHit)
        {
            this.hits++;
        }
        this.total++;
    }

    public int getHits()
    {
        return this.hits;
    }

    public int getTotal()
    {
        return this.total;
    }

    public float getHitRate()
    {
        return this.hits / (float)this.total;
    }

    @Override
    public int hashCode()
    {
        return this.hits << 16 + this.total;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof StatisticCounter))
        {
            return false;
        }
        StatisticCounter that = (StatisticCounter) other;
        return this.hits == that.hits && this.total == that.total;
    }

    @Override
    public String toString()
    {
        return "StatisticCounter[hits=" + this.hits + "; total=" + this.total + "; hitRate=" + this.getHitRate() + ']';
    }
}

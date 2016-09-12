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



import com.gs.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.util.List;

public class DateCluster
{
    private final List<Timestamp> businessDates;

    public DateCluster(List<Timestamp> businessDates)
    {
        this.businessDates = businessDates;
    }

    public DateCluster(Timestamp businessDate)
    {
        this.businessDates = FastList.newListWith(businessDate);
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

        DateCluster dateCluster = (DateCluster) o;

        if (businessDates != null ? !businessDates.equals(dateCluster.businessDates) : dateCluster.businessDates != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return businessDates != null ? businessDates.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "DateCluster{" +
                "businessDates=" + businessDates +
                '}';
    }

    public Timestamp getStartDate()
    {
        return this.businessDates.get(0);
    }

    public Timestamp getEndDate()
    {
        return this.businessDates.get(this.businessDates.size() - 1);
    }

    public List<Timestamp> getBusinessDates()
    {
        return this.businessDates;
    }

    public Timestamp getBusinessDate()
    {
        if (this.businessDates.size() == 1)
        {
            return this.businessDates.get(0);
        }
        else
        {
            return null;
        }
    }

    public int size()
    {
        return this.businessDates.size();
    }

    public boolean hasManyDates()
    {
        return this.size() > 1;
    }
}

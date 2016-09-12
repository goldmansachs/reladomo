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


import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;

import java.sql.Timestamp;
import java.util.Arrays;

public class DependentLoadingTaskSpawnerKey
{
    private final Object sourceAttribute;
    private final Timestamp businessDate;
    private final LoaderFactory helperFactory;
    private final Attribute[] attributes;
    private final Operation additionalOperation;

    public DependentLoadingTaskSpawnerKey(Object sourceAttribute, Timestamp businessDate, LoaderFactory helperFactory,
                                          Attribute[] attributes, Operation additionalOperation)
    {
        this.sourceAttribute = sourceAttribute;
        this.businessDate = businessDate;
        this.helperFactory = helperFactory;
        this.attributes = attributes;
        this.additionalOperation = additionalOperation;
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

        DependentLoadingTaskSpawnerKey that = (DependentLoadingTaskSpawnerKey) o;

        if (additionalOperation != null ? !additionalOperation.equals(that.additionalOperation) : that.additionalOperation != null)
        {
            return false;
        }
        if (!Arrays.equals(attributes, that.attributes))
        {
            return false;
        }
        if (businessDate != null ? !businessDate.equals(that.businessDate) : that.businessDate != null)
        {
            return false;
        }
        if (helperFactory != null ? !helperFactory.equals(that.helperFactory) : that.helperFactory != null)
        {
            return false;
        }
        if (sourceAttribute != null ? !sourceAttribute.equals(that.sourceAttribute) : that.sourceAttribute != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = sourceAttribute != null ? sourceAttribute.hashCode() : 0;
        result = 31 * result + (businessDate != null ? businessDate.hashCode() : 0);
        result = 31 * result + (helperFactory != null ? helperFactory.hashCode() : 0);
        result = 31 * result + (attributes != null ? Arrays.hashCode(attributes) : 0);
        result = 31 * result + (additionalOperation != null ? additionalOperation.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "DependentLoadingTaskSpawnerKey{" +
                "sourceAttribute=" + sourceAttribute +
                "businessDate=" + businessDate +
                ", helperFactory='" + helperFactory + '\'' +
                ", attributes=" + (attributes == null ? null : Arrays.asList(attributes)) +
                ", additionalOperation=" + additionalOperation +
                '}';
    }
}

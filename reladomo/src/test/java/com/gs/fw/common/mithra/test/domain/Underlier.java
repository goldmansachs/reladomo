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

package com.gs.fw.common.mithra.test.domain;

public class Underlier extends UnderlierAbstract
{
    
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof UnderlierAbstract))
        {
            return false;
        }

        final UnderlierAbstract underlierAbstract = (UnderlierAbstract) o;

        if (getContractId() != underlierAbstract.getContractId())
        {
            return false;
        }
        if (getProductId() != underlierAbstract.getProductId())
        {
            return false;
        }
        if (getUnderlierId() != underlierAbstract.getUnderlierId())
        {
            return false;
        }
        if (!getAccountId().equals(underlierAbstract.getAccountId()))
        {
            return false;
        }
        if (!getContractType().equals(underlierAbstract.getContractType()))
        {
            return false;
        }
        if (!getCurrency().equals(underlierAbstract.getCurrency()))
        {
            return false;
        }
        if (!getDescription().equals(underlierAbstract.getDescription()))
        {
            return false;
        }

        return true;
    }

    public int hashCode()
    {
        int result;
        result = getContractType().hashCode();
        result = 29 * result + getContractId();
        result = 29 * result + getAccountId().hashCode();
        result = 29 * result + getProductId();
        result = 29 * result + (int) (getUnderlierId() ^ (getUnderlierId() >>> 32));
        result = 29 * result + getDescription().hashCode();
        result = 29 * result + getCurrency().hashCode();
        return result;
    }
}

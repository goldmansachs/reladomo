
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

public class ParaDesk
extends ParaDeskAbstract
{


    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof ParaDeskAbstract))
        {
            return false;
        }

        final ParaDeskAbstract paraDeskAbstract = (ParaDeskAbstract) o;

        if (isActiveBoolean() != paraDeskAbstract.isActiveBoolean())
        {
            return false;
        }
        if (getConnectionLong() != paraDeskAbstract.getConnectionLong())
        {
            return false;
        }
        if (getLocationByte() != paraDeskAbstract.getLocationByte())
        {
            return false;
        }
        if (getMaxFloat() != paraDeskAbstract.getMaxFloat())
        {
            return false;
        }
        if (getMinShort() != paraDeskAbstract.getMinShort())
        {
            return false;
        }
        if (getSizeDouble() != paraDeskAbstract.getSizeDouble())
        {
            return false;
        }
        if (getStatusChar() != paraDeskAbstract.getStatusChar())
        {
            return false;
        }
        if (getTagInt() != paraDeskAbstract.getTagInt())
        {
            return false;
        }
        if (getClosedDate() != null ? !getClosedDate().equals(paraDeskAbstract.getClosedDate()) : paraDeskAbstract.getClosedDate() != null)
        {
            return false;
        }
        if (getCreateTimestamp() != null ? !getCreateTimestamp().equals(paraDeskAbstract.getCreateTimestamp()) : paraDeskAbstract.getCreateTimestamp() != null)
        {
            return false;
        }
        if (getDeskIdString() != null ? !getDeskIdString().equals(paraDeskAbstract.getDeskIdString()) : paraDeskAbstract.getDeskIdString() != null)
        {
            return false;
        }

        return true;
    }

    public int hashCode()
    {
        int result;
        long temp;
        result = getDeskIdString() != null ? getDeskIdString().hashCode() : 0;
        result = 29 * result + (isActiveBoolean() ? 1 : 0);
        temp = getSizeDouble() != +0.0d ? Double.doubleToLongBits(getSizeDouble()) : 0l;
        result = 29 * result + (int) (temp ^ (temp >>> 32));
        result = 29 * result + (int) (getConnectionLong() ^ (getConnectionLong() >>> 32));
        result = 29 * result + getTagInt();
        result = 29 * result + (int) getStatusChar();
        result = 29 * result + (getCreateTimestamp() != null ? getCreateTimestamp().hashCode() : 0);
        result = 29 * result + (int) getLocationByte();
        result = 29 * result + (getClosedDate() != null ? getClosedDate().hashCode() : 0);
        result = 29 * result + getMaxFloat() != +0.0f ? Float.floatToIntBits(getMaxFloat()) : 0;
        result = 29 * result + (int) getMinShort();
        return result;
    }
}

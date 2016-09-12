
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

public class DifferentDataTypes extends DifferentDataTypesAbstract
{

	public DifferentDataTypes()
	{
		super();
	}

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

        final DifferentDataTypes that = (DifferentDataTypes) o;

        if (isBooleanColumn() != that.isBooleanColumn())
        {
            return false;
        }
        if (getByteColumn() != that.getByteColumn())
        {
            return false;
        }
        if (Double.compare(that.getDoubleColumn(), getDoubleColumn()) != 0)
        {
            return false;
        }
        if (Float.compare(that.getFloatColumn(), getFloatColumn()) != 0)
        {
            return false;
        }
        if (getId() != that.getId())
        {
            return false;
        }
        if (getLongColumn() != that.getLongColumn())
        {
            return false;
        }
        if (getShortColumn() != that.getShortColumn())
        {
            return false;
        }
        if (getIntColumn() != that.getIntColumn())
        {
            return false;
        }
        return getCharColumn() == that.getCharColumn();

    }

    public int hashCode()
    {
        int result;
        long temp;
        result = (isBooleanColumn() ? 1 : 0);
        result = 29 * result + (int) getByteColumn();
        temp = getDoubleColumn() != +0.0d ? Double.doubleToLongBits(getDoubleColumn()) : 0L;
        result = 29 * result + (int) (temp ^ (temp >>> 32));
        result = 29 * result + getFloatColumn() != +0.0f ? Float.floatToIntBits(getFloatColumn()) : 0;
        result = 29 * result + getId();
        result = 29 * result + (int) (getLongColumn() ^ (getLongColumn() >>> 32));
        result = 29 * result + (int) getShortColumn();
        result = 29 * result + (int) getCharColumn();
        result = 29 * result + getIntColumn();
        return result;
    }
}

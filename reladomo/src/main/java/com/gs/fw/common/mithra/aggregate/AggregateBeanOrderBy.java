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

package com.gs.fw.common.mithra.aggregate;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.finder.OrderBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;



public class AggregateBeanOrderBy implements OrderBy
{
    private Method getterMethod;
    private boolean isAscending;

    public AggregateBeanOrderBy(Method getterMethod, boolean ascending)
    {
        this.getterMethod = getterMethod;
        this.isAscending = ascending;
    }

    public OrderBy and(OrderBy other)
    {
        return new AggregateChainedOrderBy(this, other);
    }

    public int compare(Object left, Object right)
    {
        int result;
        Object leftBeanVal = getValue(left);
        Object rightBeanVal = getValue(right);
        if (leftBeanVal == null)
        {
            if (rightBeanVal == null)
            {
                result = 0;
            }
            else
            {
                result = -1;
            }
        }
        else if (rightBeanVal == null)
        {
            result = 1;
        }
        else
        {
            result = ((Comparable) leftBeanVal).compareTo(rightBeanVal);

        }
        if (!isAscending) result = -result;
        return result;
    }

    private Object getValue(Object instance)
    {
        try
        {
            return getterMethod.invoke(instance);
        }
        catch (IllegalArgumentException e)
        {
            throw new MithraBusinessException("Invalid argument passed in invoking method " + getterMethod.getName() + "  of class " + instance.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + getterMethod.getName() + " of class " + instance.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + getterMethod.getName() + "of class " + instance.getClass().getName(), e);
        }
    }
}

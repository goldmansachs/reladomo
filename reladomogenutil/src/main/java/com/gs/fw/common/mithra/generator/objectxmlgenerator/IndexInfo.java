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

/*****************************************************************
 *
 *
 *
 *
 *
 * @(#) $Header$
 *****************************************************************/

package com.gs.fw.common.mithra.generator.objectxmlgenerator;

import java.util.ArrayList;
import java.util.Iterator;

public class IndexInfo
{
    
    private String name;
    private ArrayList<ColumnInfo> columnInfoList = null;
    private boolean unique;

    public ArrayList<ColumnInfo> getColumnInfoList()
    {
        if (this.columnInfoList == null)
        {
            this.columnInfoList = new ArrayList<ColumnInfo>();
        }

        return this.columnInfoList;
    }

    public void setColumnInfoList(ArrayList<ColumnInfo> columnInfoList)
    {
        this.columnInfoList = columnInfoList;
    }

    public boolean isUnique()
    {
        return this.unique;
    }

    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }

    public String getName()
    {
        return this.name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public boolean containsNullAttribute()
    {
        /* Indicates that this probably has an AsOfAttribute in it and shouldn't be a real index */
        for (Iterator it = this.columnInfoList.iterator(); it.hasNext();)
        {
            if (((ColumnInfo) it.next()).getAttributeName() == null)
            {
                return true;
            }

            if (((ColumnInfo) it.next()).getAttributeName().equals("null"))
            {
                return true;
            }
        }
        return false;
    }
}

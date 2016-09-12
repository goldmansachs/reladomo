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

public class AsOfAttributeNamePair
{

    private String startName = "";
    private String endName = "";

    private ColumnInfo startColumn;
    private ColumnInfo endColumn;

    private String dateType = "";

    public AsOfAttributeNamePair(String startName, String endName, String dateType)
    {
        this.startName = startName;
        this.endName = endName;

        this.dateType = dateType;
    }

    public boolean completesPair(ColumnInfo columnInfo)
    {
        boolean found = false;
        if (columnInfo.getColumnName().equals(startName))
        {
            startColumn = columnInfo;
            found = true;
        }
        else if (columnInfo.getColumnName().equals(endName))
        {
            endColumn = columnInfo;
            found = true;
        }

        return found && startColumn != null  && endColumn != null;
    }

    public boolean isAnAsOfAttribute(ColumnInfo columnInfo)
    {
        return columnInfo.getColumnName().equals(startName) || columnInfo.getColumnName().equals(endName);

    }

    public void markIncompleteAsNormalAttribute()
    {
        if (startColumn != null && endColumn == null)
        {
            startColumn.setAsOfAttribute(false);
        }
        if (startColumn == null && endColumn != null)
        {
            endColumn.setAsOfAttribute(false);
        }
    }

    public String getDateType()
    {
        return dateType;
    }

    public ColumnInfo getStartColumn()
    {
        return startColumn;
    }

    public ColumnInfo getEndColumn()
    {
        return endColumn;
    }
}

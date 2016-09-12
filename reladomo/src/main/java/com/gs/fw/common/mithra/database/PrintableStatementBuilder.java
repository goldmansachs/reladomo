
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

package com.gs.fw.common.mithra.database;

import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.finder.SqlQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class PrintableStatementBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintableStatementBuilder.class);

    private final String statementWithPlaceHolders;
    private final SqlQuery query;

    public PrintableStatementBuilder(String statementWithPlaceHolders, SqlQuery query)
    {
        this.statementWithPlaceHolders = statementWithPlaceHolders;
        this.query = query;
    }

    public String getStatementWithActualParameters()
    {
        PrintablePreparedStatement pps = new PrintablePreparedStatement(this.statementWithPlaceHolders);
        try
        {
            query.setStatementParameters(pps);
        }
        catch (SQLException e)
        {
            LOGGER.warn("Exception building statement with actual parameters. Will return statement with placeholders instead", e);
            return this.statementWithPlaceHolders;
        }
        return pps.getPrintableStatement();
    }

    public String getStatementWithPlaceHolders()
    {
        return this.statementWithPlaceHolders;
    }
}

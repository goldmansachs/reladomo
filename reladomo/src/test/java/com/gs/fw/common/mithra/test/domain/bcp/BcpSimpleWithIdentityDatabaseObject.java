

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

package com.gs.fw.common.mithra.test.domain.bcp;

import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.TableColumnInfo;

import java.sql.Connection;
import java.sql.SQLException;

public class BcpSimpleWithIdentityDatabaseObject extends BcpSimpleWithIdentityDatabaseObjectAbstract
{

    public boolean verifyTable(Object source)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        String schema = this.getSchemaGenericSource(source);
        Connection con = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TableColumnInfo tableInfo = dt.getTableColumnInfo(con, schema, "BCP_SIMPLE_IDENT");
            return tableInfo != null; // just check the table exists.
        }
        catch (SQLException e)
        {
            analyzeAndWrapSqlExceptionGenericSource("verify table failed "+e.getMessage(), e, source, con);
        }
        finally
        {
          closeConnection(con);
        }
        return false;
    }

    public void createTestTable(Object source)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        StringBuilder statement = new StringBuilder("create table ").append(this.getFullyQualifiedTableNameGenericSource(source)).append(" ( ");
        statement.append("ID integer  not null,");
        statement.append("ident integer identity,");
        statement.append("NAME varchar(24))");
        executeSqlStatementGenericSource(statement.toString(), null);
        this.createPrimaryKeyIndexForTestTable(null);
    }
}

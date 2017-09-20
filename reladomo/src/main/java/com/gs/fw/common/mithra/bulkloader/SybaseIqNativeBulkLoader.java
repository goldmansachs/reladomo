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

package com.gs.fw.common.mithra.bulkloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.gs.fw.common.mithra.attribute.OutputStreamFormatter;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;

public class SybaseIqNativeBulkLoader extends SybaseIqBulkLoader
{
    private boolean nativeClientTransferMode = false;

    public SybaseIqNativeBulkLoader(SybaseDatabaseType sybaseDatabaseType, String dbLoadDir, String appLoadDir)
    {
        super(sybaseDatabaseType, dbLoadDir, appLoadDir);
        this.setNativeClientTransferModeFromDatabaseType(sybaseDatabaseType);
    }

    private void setNativeClientTransferModeFromDatabaseType(SybaseDatabaseType sybaseDatabaseType)
    {
        String databaseTypeName = sybaseDatabaseType.getClass().getSimpleName();
        nativeClientTransferMode = databaseTypeName.toLowerCase().contains("native");
    }


    @Override
    public void execute(Connection connection) throws BulkLoaderException, SQLException
    {

        if (!this.nativeClientTransferMode)
        {
            super.execute(connection);
        }
        // transfer file using native driver and client file
        // Flush the writer to ensure everything is written out to the BCP file
        try
        {
            this.bulkOutputStream.close();
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("Could not flush the remaining output to the temporary BCP file", e);
        }

        String tableName = this.getTableMetadata().getSchema() + "." + this.getTableMetadata().getName();
        //no transfer to shared directory, so use app directory
        String loadFileLocation = this.appLoadDir + this.bulkFile.getName();
        loadFileLocation = loadFileLocation.replace("\\", "\\\\");

        String commaSeparatedColumnSpecs = this.getCommaSeparatedColumnSpecsNoLinefeeds();
        String sql = "LOAD TABLE " + tableName + " ( " + commaSeparatedColumnSpecs + " )" +
                " USING CLIENT FILE '" + loadFileLocation + "'" +
                " ESCAPES OFF" +  //Mandatory for IQ
                " QUOTES ON" + //is default value but setting it anyway
                " CHECK CONSTRAINTS OFF" + //means it does not try to check for constraints on this load - since it is temp table load, should be OK
                " BYTE ORDER LOW" + // matching the original
                " FORMAT BINARY" + //the column specs contain whether each column is binary or not, so need to use BINARY here
                " PREVIEW OFF" +  // displays the layout of input into the destination table - not needed
                " NOTIFY 10000000";  // notify after this number are successfully inserted.  In effect, disable notification

        logger.debug(sql);
        Statement stm = null;
        try
        {
            stm = connection.createStatement();
            this.setExpectedExecuteReturn(this.objectsBound);
            int numberUpdated = stm.executeUpdate(sql);

            if (numberUpdated != objectsBound)
            {
                throw new BulkLoaderException("Expecting insert of " + objectsBound + " but got " + numberUpdated);
            }
        }
        finally
        {
            if (stm != null)
                stm.close();
        }
    }

    private String getCommaSeparatedColumnSpecsNoLinefeeds()
    {
        StringBuilder buffer = new StringBuilder();
        boolean previousNullByte = false;
        for (OutputStreamFormatter columnFormatter : columnFormatters)
        {
            buffer.append(columnFormatter.getColumnSpec(previousNullByte));
            previousNullByte = columnFormatter.hasNullByte();
            buffer.append(", ");
        }
        buffer.setLength(buffer.length() - 2);
        return buffer.toString();
    }
}
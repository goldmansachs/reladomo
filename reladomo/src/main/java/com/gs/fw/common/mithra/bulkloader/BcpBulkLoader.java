
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

import org.slf4j.Logger;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.Formatter;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.execute.Execute;
import com.gs.fw.common.mithra.util.execute.LogOutputStream;
import com.gs.fw.common.mithra.util.execute.PumpStreamHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * <p><code>BulkLoader</code> implementation that uses the <code>bcp</code> command-line tool to
 * perform the bulk load.</p>

 */
public class BcpBulkLoader extends AbstractSybaseBulkLoader
{

    private static final Formatter TOSTRING_FORMATTER = new ToStringFormatter();

    private SybaseBcpFile bcpFile;
    private Formatter[] columnFormatters;
    private String user;
    private String password;

    public BcpBulkLoader(SybaseDatabaseType sybaseDatabaseType, String user, String password)
    {
        super(sybaseDatabaseType);
        this.user = user;
        this.password = password;
    }

    public void initialize(TimeZone dbTimeZone, String schema, String tableName, Attribute[] attributes,
            Logger logger, String tempTableName, String columnCreationStatement, Connection con) throws BulkLoaderException

    {
        super.initialize(dbTimeZone, schema, tableName, attributes, logger, tempTableName, columnCreationStatement, con);
        // Create the BCP file
        String[] columns = new String[attributes.length];
        for (int i = 0; i < attributes.length; i++)
        {
            Attribute attribute = attributes[i];
            columns[i] = attribute.getColumnName();
        }

        this.bcpFile = createBcpFile(columns);

        // Create the column formatters
        this.createColumnFormatters();
    }

    protected SybaseBcpFile createBcpFile(String[] columns)
            throws BulkLoaderException
    {
        try
        {
            return new SybaseBcpFile(this.tableMetadata, columns);
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("Could not create temporary BCP file", e);
        }
    }

    protected SybaseBcpFile getBcpFile()
    {
        return bcpFile;
    }

    protected void createColumnFormatters() throws BulkLoaderException
    {
        this.columnFormatters = new Formatter[this.attributes.length];

        for (int i = 0; i < this.attributes.length; i++)
        {
            Attribute attribute = this.attributes[i];
            if (attribute instanceof TimestampAttribute)
            {
                TimestampAttribute timeAttribute = (TimestampAttribute) attribute;
                this.columnFormatters[i] = new TimeZoneTimestampFormatter(SybaseDatabaseType.TIMESTAMP_FORMAT, this.dbTimeZone, timeAttribute);
            }
            else
            {
                ColumnInfo column = this.tableMetadata.getColumn(((Attribute)attribute).getColumnName());
                switch (column.getType())
                {
                    case Types.CHAR:
                        this.columnFormatters[i] = getCharFormatter(logger, column);
                        break;
                    case Types.VARCHAR:
                        this.columnFormatters[i] = getVarCharFormatter(logger,column);
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        this.columnFormatters[i] = new DecimalPlaceFormatter(column.getScale());
                        break;
                    case Types.DATE:
                        this.columnFormatters[i] = new DateFormatter(SybaseDatabaseType.TIMESTAMP_FORMAT);
                        break;
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        throw new BulkLoaderException(
                            "There is an inconsistency on " + this.tableMetadata.getName() + "." + column.getName()
                            + ". The database (" + this.databaseInfo.getServer() + "/" + this.databaseInfo.getDatabase()
                            + ") is reporting that it is a DATE/TIME/TIMESTAMP column yet the Mithra attribute is not a "
                            + "TimeAttribute. Check the mapping and/or the database to make sure they are consistent."
                        );
                    case Types.BIT:
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        this.columnFormatters[i] = new IntegerFormatter();
                        break;
                    default:
                        this.columnFormatters[i] = TOSTRING_FORMATTER;
                }
            }
        }
    }

    protected Formatter getVarCharFormatter(Logger logger, ColumnInfo column)
    {
        return new VarCharFormatter(logger, column.getName(), column.getSize());
    }

    protected Formatter getCharFormatter(Logger logger, ColumnInfo column)
    {
        return new DbCharFormatter(logger, column.getName(), column.getSize());
    }

    @Override
    public void bindObject(MithraObject object) throws BulkLoaderException
    {
        MithraDataObject obj = ((MithraTransactionalObject) object).zGetTxDataForRead();
        for (int i = 0; i < this.attributes.length; i++)
        {

            Attribute attribute = this.attributes[i];
            String columnValue = attribute.isAttributeNull(obj) ? this.formatNull() :
                    ((SingleColumnAttribute)attribute).valueOfAsString(obj, this.columnFormatters[i]);

            try
            {
                this.bcpFile.writeColumn(columnValue);
            }
            catch (IllegalArgumentException e)
            {
                throw new BulkLoaderException(e.getMessage());
            }
            catch (IOException e)
            {
                throw new BulkLoaderException("Cannot bind object to BCP data file.", e);
            }
        }
        try
        {
            this.bcpFile.endRow();
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("end row failed", e);
        }
    }

    protected String formatNull()
    {
        return null;
    }

    public void execute(Connection connection) throws BulkLoaderException, SQLException
    {
        // Flush the writer to ensure everything is written out to the BCP file
        try
        {
            this.bcpFile.close();
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("Could not flush the remaining output to the temporary BCP file", e);
        }

        Execute execute = new Execute();
        execute.setCommand(createCommand());
        execute.setTerminateOnJvmExit(true);
        execute.setWorkingDirectory(bcpFile.getDataFile().getParentFile());

        PumpStreamHandler streamHandler = new PumpStreamHandler(
            LogOutputStream.logDebug(this.logger), LogOutputStream.logWarn(this.logger)
        );
        execute.setStreamHandler(streamHandler);

        int exitCode = -1;
        try
        {
            exitCode = execute.execute();
        }
        catch (IOException e1)
        {
            throw new BulkLoaderException("Error executing BCP", e1);
        }

        if (exitCode != 0)
        {
            throw new BulkLoaderException("bcp load failed with exit code " + exitCode);
        }
    }

    private List createCommand()
    {
        List command = new ArrayList();
        command.add("bcp");

        if (this.tableMetadata.getSchema() != null)
        {
            command.add(this.tableMetadata.getSchema() + ".." + this.tableMetadata.getName());
        }
        else
        {
            command.add(this.tableMetadata.getName());
        }

        command.add("in");
        command.add(this.bcpFile.getDataFile().getName());
        command.add("-S");
        command.add(this.databaseInfo.getServer());

        command.add("-U");
        command.add(this.user);
        command.add("-P");
        command.add(this.password);

        command.add("-m");
        command.add("0");
//        command.add("-f");
//        command.add(this.bcpFile.getFormatFile().getName());
        command.add("-c");

        return command;
    }

    public void destroy()
    {
        super.destroy();
        if (this.bcpFile != null)
        {
            this.bcpFile.delete();
        }

    }
}
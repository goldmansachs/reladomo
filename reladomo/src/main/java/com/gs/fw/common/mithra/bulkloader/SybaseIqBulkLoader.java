
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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.OutputStreamFormatter;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.TempTableNamer;
import org.slf4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.TimeZone;

public class SybaseIqBulkLoader extends AbstractSybaseBulkLoader
{
    /* from 1.6 version of java.sql.Types for 1.5 compatibility */
    public static final int NCHAR = -15;
    public static final int NVARCHAR = -9;
    public static final int LONGNVARCHAR = -16;
    public static final int NCLOB = 2011;
    
    private String dbLoadDir;
    protected String appLoadDir;
    protected File bulkFile;
    protected OutputStream bulkOutputStream;
    protected OutputStreamFormatter[] columnFormatters;
    protected int objectsBound;

    public SybaseIqBulkLoader(SybaseDatabaseType sybaseDatabaseType, String dbLoadDir, String appLoadDir)
    {
        super(sybaseDatabaseType);
        this.dbLoadDir = dbLoadDir;
        this.appLoadDir = appLoadDir;
    }

    @Override
    public void initialize(TimeZone dbTimeZone, String schema, String tableName, Attribute[] attributes,
            Logger logger, String tempTableName, String columnCreationStatement, Connection con) throws BulkLoaderException
    {
        super.initialize(dbTimeZone, schema, tableName, attributes, logger, tempTableName, columnCreationStatement, con);

        createBulkLoadFile();

        // Create the column formatters
        this.createColumnFormatters();
    }
    protected void createColumnFormatters() throws BulkLoaderException
    {
        this.columnFormatters = new OutputStreamFormatter[this.attributes.length];

        for (int i = 0; i < this.attributes.length; i++)
        {
            Attribute attribute = this.attributes[i];
            ColumnInfo column = this.tableMetadata.getColumn(attribute.getColumnName());
            switch (column.getType())
            {
                case Types.CHAR:
                case NCHAR:
                    this.columnFormatters[i] = new SybaseIqFixedCharFormatter(column.isNullable(), column.getName(), column.getSize());
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                case NVARCHAR:
                case LONGNVARCHAR:
                case NCLOB:
                    this.columnFormatters[i] = new SybaseIqVarCharFormatter(column.isNullable(), column.getName(), column.getSize());
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    this.columnFormatters[i] = new SybaseIqDecimalFormatter(column.isNullable(), column.getName(), column.getScale());
                    break;
                case Types.DATE:
                    this.columnFormatters[i] = new SybaseIqDateFormatter(column.isNullable(), column.getName());
                    break;
                case Types.TIME:
                case Types.TIMESTAMP:
                    if (attribute instanceof TimestampAttribute)
                    {
                        TimestampAttribute timeAttribute = (TimestampAttribute) attribute;
                        this.columnFormatters[i] = new SybaseIqTimestampWithZoneFormatter(column.isNullable(), column.getName(), this.dbTimeZone, timeAttribute);
                    }
                    else
                    {
                        this.columnFormatters[i] = new SybaseIqTimestampFormatter(column.isNullable(), column.getName());
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    this.columnFormatters[i] = new SybaseIqBooleanFormatter(column.isNullable(), column.getName());
                    break;
                case Types.INTEGER:
                    this.columnFormatters[i] = new SybaseIqIntFormatter(column.isNullable(), column.getName());
                    break;
                case Types.TINYINT:
                    this.columnFormatters[i] = new SybaseIqByteFormatter(column.isNullable(), column.getName());
                    break;
                case Types.SMALLINT:
                    this.columnFormatters[i] = new SybaseIqShortFormatter(column.isNullable(), column.getName());
                    break;
                case Types.BIGINT:
                    this.columnFormatters[i] = new SybaseIqLongFormatter(column.isNullable(), column.getName());
                    break;
                case Types.DOUBLE:
                    this.columnFormatters[i] = new SybaseIqDoubleFormatter(column.isNullable(), column.getName());
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    this.columnFormatters[i] = new SybaseIqFloatFormatter(column.isNullable(), column.getName());
                    break;
                default:
                    throw new RuntimeException("Can't handle data of type "+column.getType()+" for column "+column.getName()+" in table "+this.tableMetadata.getName());
            }
        }
    }

    @Override
    public void bindObject(MithraObject object) throws BulkLoaderException
    {
        MithraDataObject obj = ((MithraTransactionalObject) object).zGetTxDataForRead();
        try
        {
            for(int i=0;i<this.attributes.length;i++)
            {
                    if (attributes[i].isAttributeNull(obj))
                    {
                        this.columnFormatters[i].writeNull(this.bulkOutputStream);
                    }
                    else
                    {
                        ((SingleColumnAttribute)this.attributes[i]).writeValueToStream(obj, this.columnFormatters[i], this.bulkOutputStream);
                    }
            }
            objectsBound++;
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("error while writing to file ", e);
        }
    }

    protected void createBulkLoadFile()
            throws BulkLoaderException
    {
        String temporaryFilePrefix = tableMetadata.getName() + TempTableNamer.getNextTempTableName();
        try
        {
            this.bulkFile = new File(appLoadDir, temporaryFilePrefix+".dat");
            this.bulkOutputStream = new BufferedOutputStream(new FileOutputStream(this.bulkFile), 32*1024);
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("Could not create temporary bulk load file", e);
        }
    }

    @Override
    public void execute(Connection connection) throws BulkLoaderException, SQLException
    {
        // Flush the writer to ensure everything is written out to the BCP file
        try
        {
            this.bulkOutputStream.close();
        }
        catch (IOException e)
        {
            throw new BulkLoaderException("Could not flush the remaining output to the temporary BCP file", e);
        }

        transferFile(this.bulkFile.getName(), this.appLoadDir, this.dbLoadDir);

        String tableName = this.getTableMetadata().getSchema() + "." + this.getTableMetadata().getName();
        String sql =
                "load table "+tableName+" ( "+this.getCommaSeparatedColumnNames()+" )"+
                " from '"+this.dbLoadDir+this.bulkFile.getName()+ '\'' +
                " escapes off"+
                " notify 10000000"+
                " preview off"+
                " byte order low"+
                " format binary"+
                " quotes on";
        logger.debug(sql);
        Statement stm = null;
        try
        {
            stm = connection.createStatement();
            this.setExpectedExecuteReturn(this.objectsBound);
            int numberUpdated = stm.executeUpdate(sql);
            if (numberUpdated != objectsBound)
            {
                throw new BulkLoaderException("Expecting insert of "+objectsBound+" but got "+numberUpdated);
            }
        }
        finally
        {
            if (stm != null) stm.close();
        }
    }

    protected void setExpectedExecuteReturn(int expected)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            tx.setExpectedExecuteReturn(expected);
        }
    }

    protected void transferFile(String fileName, String appLoadDir, String dbLoadDir)
    {
        // for subclasses to extend. The default implementation assumes the disk is shared between the app server and the db server.
    }

    private String getCommaSeparatedColumnNames()
    {
        StringBuilder buffer = new StringBuilder();
        boolean previousNullByte = false;
        for (int i = 0; i < columnFormatters.length; i++)
        {
            buffer.append('\n');
            if (i > 0) buffer.append(',');
            buffer.append(columnFormatters[i].getColumnSpec(previousNullByte));
            previousNullByte = columnFormatters[i].hasNullByte();
        }
        return buffer.toString();
    }

    @Override
    public void destroy()
    {
        super.destroy();
        if (this.bulkFile != null)
        {
            this.bulkFile.delete();
        }
    }
}

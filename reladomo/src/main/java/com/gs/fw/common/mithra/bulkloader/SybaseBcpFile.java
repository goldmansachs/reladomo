
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
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.util.TableColumnInfo;
import com.gs.fw.common.mithra.util.TempTableNamer;

import java.io.*;


public class SybaseBcpFile
{

    private static final Logger logger = LoggerFactory.getLogger(SybaseBcpFile.class.getName());

    private static final String FIELD_TERMINATOR = "\t";

    private final String[] columns;
    private final boolean[] nullableColumns;

    private final File dataFile;
    private final Writer dataFileWriter;
    private int currentColumn = 0;

    public SybaseBcpFile(TableColumnInfo tableMetadata, String[] columns) throws IOException
    {
        this(tableMetadata, columns, null);
    }

    protected SybaseBcpFile(TableColumnInfo tableMetadata, String[] columns, String loadDirectory) throws IOException
    {
        this.columns = columns;

        // Create the temporary files
        String temporaryFilePrefix = tableMetadata.getName() + TempTableNamer.getNextTempTableName();

        this.dataFile = createDataFile(temporaryFilePrefix, loadDirectory);
        this.dataFileWriter = new BufferedWriter(new FileWriter(this.dataFile));

        if (logger.isDebugEnabled())
        {
            logger.debug("Temporary BCP file is located at '" + this.dataFile.getAbsolutePath() + "'");
        }

        // Map which columns are nullable
        this.nullableColumns = this.mapNullableColumns(tableMetadata);
    }

    protected File createDataFile(String temporaryFilePrefix, String loadDirectory)
            throws IOException
    {
        return File.createTempFile(temporaryFilePrefix, ".bcp");
    }

    /**
     * Maps which columns accept <code>null</code> and which don't.
     * @param tableMetadata The metadata about the table.
     * @return An array of the size of the number of columns with <code>true</code> set if the column is nullable.
     */
    private boolean[] mapNullableColumns(TableColumnInfo tableMetadata)
    {
        boolean[] nullableColumns = new boolean[this.columns.length];

        for (int i = 0; i < this.columns.length; i++)
        {
            String column = columns[i];
            nullableColumns[i] = tableMetadata.getColumn(column).isNullable();
        }

        return nullableColumns;
    }

    /**
     * write a single column. After all the columns on a single row have been written, call endRow()
     * @param value
     * @throws IllegalArgumentException if the row values contains a null value but the column isn't nullable.
     * @throws IOException if there was a problem writing to the file.
     */
    public void writeColumn(String value) throws IllegalArgumentException, IOException
    {
        if (value == null)
        {
            if (! this.nullableColumns[this.currentColumn])
            {
                throw new IllegalArgumentException(
                    "Bulk load will fail as a null value cannot be assigned to column '" + this.columns[this.currentColumn] + "'"
                );
            }
        }
        else
        {
            this.dataFileWriter.write(value);
        }

        if (this.currentColumn < (this.columns.length - 1))
        {
            this.dataFileWriter.write(FIELD_TERMINATOR);
        }
        this.currentColumn++;
    }

    public void endRow()
            throws IOException
    {
        if (this.currentColumn < this.columns.length)
        {
            throw new IOException("missing column. expected "+this.columns.length+" but only got "+this.currentColumn);
        }
        this.dataFileWriter.write('\n');
        this.currentColumn = 0;
    }

    /**
     * <p>Closes the streams to the data file.</p>
     * <p>Subsequent calls to {@link #writeColumn(String)} will fail after this is called.</p>
     * @throws IOException if there was a problem closing the data file.
     */
    public void close() throws IOException
    {
        this.dataFileWriter.flush();
        this.dataFileWriter.close();
    }

    /**
     * @return The data file.
     */
    public File getDataFile()
    {
        return this.dataFile;
    }

    /**
     * Deletes the BCP files.
     */
    public void delete()
    {
        this.dataFile.delete();
    }
}

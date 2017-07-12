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

package com.gs.fw.common.mithra.databasetype;


import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.bulkloader.SybaseIqNativeBulkLoader;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import org.joda.time.LocalDateTime;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

public class SybaseIqNativeDatabaseType extends SybaseIqDatabaseType
{
    public static final int SQL_CODE_UNIQUE_INDEX_VIOLATION = -1002003;

    private final static SybaseIqNativeDatabaseType instance = new SybaseIqNativeDatabaseType(true);
    private final static SybaseIqNativeDatabaseType instanceUnsharedTempTable = new SybaseIqNativeDatabaseType(false);

    public static SybaseIqNativeDatabaseType getInstance()
    {
        return instance;
    }

    public static SybaseIqNativeDatabaseType getInstanceWithoutSharedTempTables()
    {
        return instanceUnsharedTempTable;
    }

    public SybaseIqNativeDatabaseType(boolean supportsSharedTempTables)
    {
        super(supportsSharedTempTables);
    }

    @Override
    public BulkLoader createBulkLoader(String dbLoadDir, String appLoadDir) throws BulkLoaderException
    {
        return new SybaseIqNativeBulkLoader(this, dbLoadDir, appLoadDir);
    }

    @Override
    public Timestamp getTimestampFromResultSet(ResultSet rs, int pos, TimeZone timeZone) throws SQLException
    {
        Timestamp localTs = rs.getTimestamp(pos);
        if (localTs == null)
        {
            return null;
        }
        LocalDateTime ldt = new LocalDateTime(localTs.getTime());

        Calendar utcCal = getCalendarInstance();
        utcCal.set(Calendar.YEAR, ldt.getYear());
        utcCal.set(Calendar.MONTH, ldt.getMonthOfYear() - 1);
        utcCal.set(Calendar.DAY_OF_MONTH, ldt.getDayOfMonth());
        utcCal.set(Calendar.HOUR_OF_DAY, ldt.getHourOfDay());
        utcCal.set(Calendar.MINUTE, ldt.getMinuteOfHour());
        utcCal.set(Calendar.SECOND, ldt.getSecondOfMinute());
        utcCal.set(Calendar.MILLISECOND, ldt.getMillisOfSecond());
        Timestamp utcTs = new Timestamp(utcCal.getTimeInMillis());
        return MithraTimestamp.zConvertTimeForReadingWithUtcCalendar(utcTs, timeZone);
    }

    @Override
    public String getTempDbSchemaName()
    {
        return null;
    }

    @Override
    public boolean violatesUniqueIndexWithoutRecursion(SQLException next)
    {
        return next.getErrorCode() == SQL_CODE_UNIQUE_INDEX_VIOLATION;
    }

    @Override
    public String getCurrentSchema(Connection con) throws SQLException
    {
        return null;
    }

    @Override
    public void setSchemaOnConnection(Connection con, String schema) throws SQLException
    {
    }

    public static void loadNativeDrivers()
    {
        // see: http://dcx.sybase.com/index.html#1201/en/dbprogramming/jdbc-driver-deploy.html
        // http://dcx.sybase.com/index.html#sa160/en/dbprogramming/jdbc-client-deploy.html
        boolean windows = System.getProperty("os.name").toUpperCase().startsWith("WIN");
        boolean sixtyFourBit = System.getProperty("os.arch").contains("64") || System.getProperty("java.vm.name").contains("64-Bit");
        String destinationPath = "";//sixtyFourBit ? "Bin64" : "bin32";
        if (windows)
        {
            try
            {
                String prefix = sixtyFourBit ? "/iqnative/win64/" : "/iqnative/win32";
                File location = setupLibrariesFromJar(destinationPath, prefix + "dblgen17.dll",
                        prefix + "dbicu17.dll", prefix + "dbicudt17.dll", prefix + "dbjdbc17.dll");
                new File(location.getAbsolutePath()+File.separator+destinationPath).deleteOnExit();
                location.deleteOnExit();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not load native libraries", e);
            }
        }
        else
        {
            throw new RuntimeException("In UNIX, you must install the .so files and change your LD_LIBRARY_PATH to point to the installed directory");
        }
    }

    public static File setupLibrariesFromJar(String destinationPath, String... paths) throws IOException
    {
        File temp = setupLibDir();
        System.setProperty("asa.location", temp.getAbsolutePath()+File.separator);
        for(String s: paths)
        {
            writeLib(destinationPath, s, temp);
        }
        return temp;
    }

    private static String writeLib(String destinationDir, String path, File temp) throws IOException
    {
        if (!path.startsWith("/"))
        {
            throw new IllegalArgumentException("The path has to be absolute (start with '/').");
        }
        File dir = new File(temp.getAbsolutePath()+"/"+destinationDir);
        dir.mkdirs();

        String[] parts = path.split("/");
        String filename = (parts.length > 1) ? parts[parts.length - 1] : null;
        String filenameWithoutExtension = filename.substring(0, filename.lastIndexOf("."));

        File outFile = new File(temp.getAbsolutePath()+"/"+destinationDir+"/"+filename);
        byte[] buffer = new byte[1024];
        int readBytes;

        InputStream is = SybaseIqNativeDatabaseType.class.getResourceAsStream(path);
        if (is == null)
        {
            throw new FileNotFoundException("File " + path + " was not found inside JAR.");
        }

        OutputStream os = new FileOutputStream(outFile);
        try
        {
            while ((readBytes = is.read(buffer)) != -1)
            {
                os.write(buffer, 0, readBytes);
            }
        }
        finally
        {
            os.close();
            is.close();
        }
        outFile.deleteOnExit();
        return filenameWithoutExtension;
    }

    public static File setupLibDir() throws IOException
    {
        File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if(!(temp.delete()))
        {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if(!(temp.mkdir()))
        {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        temp.deleteOnExit();
        return temp;
    }
}

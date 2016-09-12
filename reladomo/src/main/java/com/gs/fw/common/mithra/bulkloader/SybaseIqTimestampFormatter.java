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
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;


public class SybaseIqTimestampFormatter extends SybaseIqOutputFormatter
{

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public SybaseIqTimestampFormatter(boolean nullable, String columnName)
    {
        super(nullable, columnName, "datetime");
    }

    public void writeNull(OutputStream os) throws IOException
    {
        checkNullability();
        for(int i=0;i<"yyyy-MM-dd HH:mm:ss.SSS".length();i++)
        {
            os.write(0);
        }
    }

    @Override
    public void write(Object obj, OutputStream os) throws IOException
    {
        if (obj instanceof Date)
        {
            os.write(dateFormat.format((Date)obj).getBytes(ISO_8859_1));
        }
        else
        {
            throwConversionError(obj.getClass().getName());
        }
    }

    public String getColumnSpec(boolean previousNullByte)
    {
        String result = this.getColumnName() + " DATE ('YYYY-MM-DD HH:NN:SS.SSS')";
        if (this.isNullable())
        {
            result += " NULL(ZEROS)";
        }
        return result;
    }
}

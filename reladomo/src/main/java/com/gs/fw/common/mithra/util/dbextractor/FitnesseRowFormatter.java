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

package com.gs.fw.common.mithra.util.dbextractor;

import com.gs.fw.common.mithra.util.Function;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.NumberFormat;



public class FitnesseRowFormatter implements Function<Object, String>
{
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final NumberFormat NUMBER_FORMAT = new DecimalFormat("#.#####");

    public String valueOf(Object object)
    {
        if (object == null)
        {
            return "";
        }
        else if (object instanceof Boolean)
        {
            return object.toString();
        }
        else if (object instanceof Number)
        {
            return NUMBER_FORMAT.format(Double.valueOf(object.toString()));
        }
        else if (object instanceof Timestamp)
        {
            return DATE_FORMAT.print(((Timestamp)object).getTime());
        }
        else
        {
            return object.toString();
        }

    }
}

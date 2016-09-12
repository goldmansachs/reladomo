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

package com.gs.fw.common.mithra;



public class MithraNullPrimitiveException extends MithraBusinessException
{

    public MithraNullPrimitiveException(String message)
    {
        super(message);
    }

    public MithraNullPrimitiveException(String message, Throwable nestedException)
    {
        super(message, nestedException);
    }

    public static void throwNew(String attributeName, MithraDataObject data)
    {
        throw new MithraNullPrimitiveException("attribute '"+attributeName+"' is null in database and a default is not specified in mithra xml for primary key / "+data.zGetPrintablePrimaryKey());
    }
}

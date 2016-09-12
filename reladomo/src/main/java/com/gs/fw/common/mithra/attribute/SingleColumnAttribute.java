
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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;


public interface SingleColumnAttribute<T>
{

    public void setColumnName(String columnName);

    public String getColumnName();

    public String valueOfAsString(T object, Formatter formatter);

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException;

    public boolean verifyColumn(ColumnInfo info);

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable);

    // the dataObject is an instance of MithraDataObject or a tuple. It's not an instance of Owner
    // the implementation has to cast it to Owner, which is not a real cast.
    public void setSqlParameters(PreparedStatement ps, Object dataObject, int position, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException;

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext);

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException;

    public abstract void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException;
    public abstract void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException;
    public abstract Object zDecodeColumnarData(ColumnarInStream in, int count) throws IOException;
    public abstract void zWritePlainTextFromColumnar(Object columnData, int row, ColumnarOutStream out) throws IOException;
}


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

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;


public interface OutputStreamFormatter
{

    public void write(Object obj, OutputStream os) throws IOException;

    public void write(BigDecimal obj, OutputStream os) throws IOException;

    public void write(boolean b, OutputStream os) throws IOException;

    public void write(byte b, OutputStream os) throws IOException;

    public void write(char c, OutputStream os) throws IOException;

    public void write(double d, OutputStream os) throws IOException;

    public void write(float f, OutputStream os) throws IOException;

    public void write(int i, OutputStream os) throws IOException;

    public void write(long l, OutputStream os) throws IOException;

    public void write(short s, OutputStream os) throws IOException;

    public void writeNull(OutputStream os) throws IOException;

    public String getColumnSpec(boolean previousNullByte);

    public boolean hasNullByte();
}

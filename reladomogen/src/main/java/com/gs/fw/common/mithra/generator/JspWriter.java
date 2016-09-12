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

package com.gs.fw.common.mithra.generator;

import java.io.*;


public class JspWriter extends PrintWriter
{

    public JspWriter(OutputStream out)
    {
        super(out);
    }

    public int getBufferSize()
    {
        return 0;
    }

    public void clearBuffer() throws IOException
    {
        //
    }

    public void writeEndOfLine()
    {
        this.write("\n");
    }

    public void writeMany2(String first, Object second)
    {
        this.write(first);
        this.print(second);
    }

    public void writeMany3(String first, Object second, String third)
    {
        this.write(first);
        this.print(second);
        this.write(third);
    }

    public void writeMany4(String first, Object second, String third, Object forth)
    {
        this.write(first);
        this.print(second);
        this.write(third);
        this.print(forth);
    }

    public void writeMany5(String first, Object second, String third, Object forth, String fifth)
    {
        this.write(first);
        this.print(second);
        this.write(third);
        this.print(forth);
        this.write(fifth);
    }

    public void writeMany6(String first, Object second, String third, Object forth, String fifth, Object sixth)
    {
        this.write(first);
        this.print(second);
        this.write(third);
        this.print(forth);
        this.write(fifth);
        this.print(sixth);
    }
}


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


public class ToStringFormatter implements Formatter
{

    public String format(Object obj)
    {
        return obj == null ? "" : obj.toString();
    }

    public String format(boolean b)
    {
        return Boolean.toString(b);
    }

    public String format(byte b)
    {
        return Byte.toString(b);
    }

    public String format(char c)
    {
        return Character.toString(c);
    }

    public String format(double d)
    {
        return Double.toString(d);
    }

    public String format(float f)
    {
        return Float.toString(f);
    }

    public String format(int i)
    {
        return Integer.toString(i);
    }

    public String format(long l)
    {
        return Long.toString(l);
    }

    public String format(short s)
    {
        return Short.toString(s);
    }
}

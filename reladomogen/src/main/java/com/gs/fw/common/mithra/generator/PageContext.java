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

import java.io.PrintWriter;


public class PageContext
{

    private JspWriter writer;

    public PageContext(JspWriter writer)
    {
        this.writer = writer;
    }

    public JspWriter getOut()
    {
        return this.writer;
    }

    public void handlePageException(Throwable t)
    {
        t.printStackTrace();
        throw new RuntimeException("error in code generation "+t.getClass().getName()+": "+t.getMessage(), t);
    }
}

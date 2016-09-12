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


public class JspFactory
{

    public PageContext getPageContext(MithraTemplate template, HttpServletRequest request , HttpServletResponse response,
      			String ignore, boolean ignore2, int ignore3, boolean ignore4)
    {
        return new PageContext(response.getWriter());
    }

    public static JspFactory getDefaultFactory()
    {
        return new JspFactory();
    }

    public void releasePageContext(PageContext context)
    {
        
    }
}

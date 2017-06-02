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

package com.gs.fw.common.mithra.finder.paramop;

import com.gs.fw.common.mithra.cache.bean.BeanByteExtractor;
import com.gs.fw.common.mithra.extractor.ByteExtractor;

public class OpWithByteParamExtractor extends BeanByteExtractor
{
    public static final ByteExtractor INSTANCE = new OpWithByteParamExtractor();

    @Override
    public byte byteValueOf(Object o)
    {
        return ((OpWithByteParam)o).getParameter();
    }
}

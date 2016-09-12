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

package com.gs.fw.common.mithra.cache.bean;


import com.gs.fw.common.mithra.extractor.AbstractByteArrayExtractor;

import java.sql.Timestamp;

public abstract class BeanByteArrayExtractor extends AbstractByteArrayExtractor
{
    @Override
    public void setByteArrayValue(Object o, byte[] newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setValue(Object o, Object newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        return false;
    }
}

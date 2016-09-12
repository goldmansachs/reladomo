
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

package com.gs.fw.common.mithra.extractor;

import java.sql.Timestamp;

public interface Extractor<T, V> extends HashableValueSelector<T, V>
{

    public void setValue(T o, V newValue);

    public void setValueNull(T o);

    public void setValueUntil(T o, V newValue, Timestamp exclusiveUntil);

    public void setValueNullUntil(T o, Timestamp exclusiveUntil);

    public boolean isAttributeNull(T o);

    public <O> boolean valueEquals(T first, O second, Extractor<O, V> secondExtractor);
}

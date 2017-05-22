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

package com.gs.reladomo.metadata;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;

public class PrivateReladomoClassMetaData extends ReladomoClassMetaData
{
    private Object asOfAttributes;
    private Attribute[] persistentAttributes;
    private Attribute[] primaryKeyAttributes;

    public PrivateReladomoClassMetaData(Class finderClass)
    {
        super(finderClass);
    }

    public PrivateReladomoClassMetaData(RelatedFinder finder)
    {
        super(finder);
    }

    @Override
    public boolean isDated()
    {
        return this.getCachedAsOfAttributes() != null;
    }

    public AsOfAttribute[] getCachedAsOfAttributes()
    {
        Object result = this.asOfAttributes;
        if (result == null)
        {
            result = getAsOfAttributes();
            this.asOfAttributes = result == null ? NONE : result;
        }
        if (result == NONE)
        {
            return null;
        }
        return (AsOfAttribute[]) result;
    }

    public Attribute[] getCachedPersistentAttributes()
    {
        Attribute[] result = this.persistentAttributes;
        if (result == null)
        {
            result = this.getPersistentAttributes();
            this.persistentAttributes = result;
        }
        return result;
    }

    public Attribute[] getCachedPrimaryKeyAttributes()
    {
        Attribute[] result = this.primaryKeyAttributes;
        if (result == null)
        {
            result = this.getPrimaryKeyAttributes();
            this.primaryKeyAttributes = result;
        }
        return result;
    }

}

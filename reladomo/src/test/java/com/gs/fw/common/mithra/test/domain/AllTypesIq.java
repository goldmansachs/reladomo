
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

package com.gs.fw.common.mithra.test.domain;

public class AllTypesIq
extends AllTypesIqAbstract
{
	public AllTypesIq()
	{
		super();
	}

/* for testing only. equals should not be implemented like this for production code*/
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof AllTypesIqAbstract))
        {
            return false;
        }

        final AllTypesIqAbstract allTypesIqAbstract = (AllTypesIqAbstract) obj;

        return !this.zGetNonTxData().changed(allTypesIqAbstract.zGetNonTxData());
    }

    public int hashCode()
    {
        return this.getId();
    }
}

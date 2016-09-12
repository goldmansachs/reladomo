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

/* GOLDMAN SACHS & CO.
 * $Id: ObjectSequence.java 19 2012-05-10 22:55:20Z beevma $
 */
package kata.domain;

import com.gs.fw.common.mithra.MithraSequence;

public class ObjectSequence
        extends ObjectSequenceAbstract
        implements MithraSequence
{
    public ObjectSequence()
	{
		super();
	}

    public void setSequenceName(String sequenceName)
    {
        this.setSimulatedSequenceName(sequenceName);
    }

    public long getNextId()
    {
        return this.getNextValue();
    }

    public void setNextId(long nextValue)
    {
        this.setNextValue(nextValue);
    }
}


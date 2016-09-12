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

import com.gs.fw.common.mithra.MithraPrimaryKeyGenerator;
import com.gs.fw.common.mithra.MithraSequence;
import com.gs.fw.common.mithra.MithraSequenceObjectFactory;
import com.gs.fw.common.mithra.SimulatedSequencePrimaryKeyGenerator;

public class ProjitoSimulatedSequenceFactory implements MithraSequenceObjectFactory
{
    private static SimulatedSequencePrimaryKeyGenerator getSimulatedSequencePrimaryKeyGeneratorForSequenceName(String sequenceName)
    {
        return MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGeneratorForNoSourceAttribute(sequenceName, ProjitoSimulatedSequenceFactory.class.getName(), null);
    }

    public static long getNextIdForSequenceName(String sequenceName)
    {
        return getSimulatedSequencePrimaryKeyGeneratorForSequenceName(sequenceName).getNextId(null);
    }

    @Override
    public MithraSequence getMithraSequenceObject(String sequenceName, Object sourceAttribute, int initialValue)
    {
        ProjitoSimulatedSequence result = ProjitoSimulatedSequenceFinder.findOne(ProjitoSimulatedSequenceFinder.sequenceName().eq(sequenceName));

        if (null == result)
        {
            result = new ProjitoSimulatedSequence();
            result.setSequenceName(sequenceName);
            result.setNextId(initialValue);
            result.insert();
        }

        return result;
    }
}

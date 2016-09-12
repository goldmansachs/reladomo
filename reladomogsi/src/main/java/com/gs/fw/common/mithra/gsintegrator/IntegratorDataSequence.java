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

package com.gs.fw.common.mithra.gsintegrator;

public class IntegratorDataSequence extends IntegratorDataSequenceAbstract
{

    public boolean isDuplicate(String currentMessageId, int currentMessageSeqNo)
    {
        int messageIdCompare = this.getId().compareTo(currentMessageId);
        if (messageIdCompare > 0)
        {
            return true;
        }
        if (messageIdCompare == 0)
        {
            return this.getSeqNo() >= currentMessageSeqNo;
        }
        return false;
    }
}

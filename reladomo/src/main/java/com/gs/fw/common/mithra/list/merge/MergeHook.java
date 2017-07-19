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

package com.gs.fw.common.mithra.list.merge;

public class MergeHook<E>
{
    public static final MergeHook DEFAULT = new MergeHook();

    public enum UpdateInstruction
    {
        UPDATE, DO_NOT_UPDATE, TERMINATE_AND_INSERT_INSTEAD;
    }

    public UpdateInstruction matchedNoDifference(E existing, E incoming)
    {
        return UpdateInstruction.DO_NOT_UPDATE;
    }

    /**
     * This method can decide not to update based on business logic, or
     * set some special values on the incoming (e.g. a creator, a timestamp, etc)
     * so that value gets updated
     * @param existing The existing object that is matched. If this method is overwritten, it must not update, or terminate this object.
     * @param incoming
     * @return
     */
    public UpdateInstruction matchedWithDifferenceBeforeAttributeCopy(E existing, E incoming)
    {
        return UpdateInstruction.UPDATE;
    }

    public enum InsertInstruction
    {
        INSERT, DO_NOT_INSERT;
    }

    public InsertInstruction beforeInsertOfNew(E newObject)
    {
        return InsertInstruction.INSERT;
    }

    public enum DeleteOrTerminateInstruction
    {
        DELETE_OR_TERMINATE, DO_NOT_DELETE_OR_TERMINATE;
    }

    public DeleteOrTerminateInstruction beforeDeleteOrTerminate(E existing, MergeBuffer<E> mergeBuffer)
    {
        return DeleteOrTerminateInstruction.DELETE_OR_TERMINATE;
    }
}

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

package com.gs.fw.common.mithra.test.util.serializer;

import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;
import com.gs.fw.common.mithra.util.serializer.SerializationConfig;

import java.io.IOException;

public class AppendableSerialContext extends ReladomoSerializationContext
{
    private Appendable appendable;
    private boolean hasFields = false;

    public AppendableSerialContext(SerializationConfig serializationConfig, SerialWriter writer, Appendable appendable)
    {
        super(serializationConfig, writer);
        this.appendable = appendable;
    }

    public Appendable getAppendable()
    {
        return appendable;
    }

    public void startField() throws IOException
    {
        if (this.hasFields)
        {
            this.appendable.append(',');
        }
        this.hasFields = true;
    }

    public void startObject()
    {
        this.hasFields = false;
    }

    public void endObject()
    {
        this.hasFields = true;
    }
}

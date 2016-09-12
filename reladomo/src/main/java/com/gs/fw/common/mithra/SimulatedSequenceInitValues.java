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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;

import java.util.ArrayList;
import java.util.List;

/*
 *******************************************************************************
 * File:        : $Source :$
 *
 *
 *
 *******************************************************************************
 */

public class SimulatedSequenceInitValues
{
    private String sequenceName;
    private int batchSize;
    private int incrementSize;
    private int initialValue;
    private String sequenceObjectFactoryName;
    private Object sequenceObjectFactory;
    private boolean hasSourceAttribute;
    private SourceAttributeType sourceAttributeType;
    private List primaryKeyAttributes = new ArrayList();
    private Attribute primaryKeyAttribute;

    public SimulatedSequenceInitValues(String sequenceName, int batchSize, int incrementSize, int initialValue, String sequenceObjectFactoryName,
            boolean hasSourceAttribute, Attribute primaryKeyAttribute, SourceAttributeType sourceAttributeType)
    {
        this.sequenceName = sequenceName;
        this.batchSize = batchSize;
        this.incrementSize = incrementSize;
        this.initialValue = initialValue;
        this.sequenceObjectFactoryName = sequenceObjectFactoryName;
        try
        {
            this.sequenceObjectFactory = Class.forName(sequenceObjectFactoryName).newInstance();
        }
        catch (Exception e)
        {
            throw new MithraException("Error intantiating MithraSequenceObjectFactory: " + sequenceObjectFactoryName, e);
        }
        this.hasSourceAttribute = hasSourceAttribute;
        this.primaryKeyAttribute = primaryKeyAttribute;
        this.sourceAttributeType = sourceAttributeType;

    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    public String getSequenceName()
    {
        return this.sequenceName;

    }

    public boolean getHasSourceAttribute()
    {
        return this.hasSourceAttribute;
    }

    public String getSequenceObjectFactoryName()
    {
        return this.sequenceObjectFactoryName;
    }

    public int getIncrementSize()
    {
        return this.incrementSize;
    }

    public int getInitialValue()
    {
        return this.initialValue;
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return sourceAttributeType;
    }

    public List getPrimaryKeyAttributes()
    {
        return primaryKeyAttributes;
    }

    public Object getSequenceObjectFactory()
    {
        return sequenceObjectFactory;
    }

    public Attribute getPrimaryKeyAttribute()
    {
        return primaryKeyAttribute;
    }
}

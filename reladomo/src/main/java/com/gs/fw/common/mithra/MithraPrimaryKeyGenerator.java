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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.attribute.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MithraPrimaryKeyGenerator
{
    private static final MithraPrimaryKeyGenerator instance = new MithraPrimaryKeyGenerator();
    private static final Logger logger = LoggerFactory.getLogger(MithraPrimaryKeyGenerator.class.getName());
    private Map<CustomKey, SimulatedSequencePrimaryKeyGenerator> simulatedSequencePrimaryKeyGenerators = new HashMap<CustomKey, SimulatedSequencePrimaryKeyGenerator>();
    private Map<CustomKey, MaxFromTablePrimaryKeyGenerator> maxFromTablePrimaryKeyGenerators = new HashMap<CustomKey, MaxFromTablePrimaryKeyGenerator>();
    private Map<CustomKey, SimulatedSequenceInitValues> uninitializedSequencePrimaryKeyGenerators = new HashMap<CustomKey, SimulatedSequenceInitValues>();

    private MithraPrimaryKeyGenerator()
    {

    }

    public static MithraPrimaryKeyGenerator getInstance()
    {
        return instance;
    }

    public synchronized void initializeSimulatedSequencePrimaryKeyGenerator(SimulatedSequenceInitValues initValues)
            throws MithraException
    {
        String sequenceName = initValues.getSequenceName();
        String sequenceFactory = initValues.getSequenceObjectFactoryName();
        Attribute primaryKeyAttribute = initValues.getPrimaryKeyAttribute();
        CustomKey key = new CustomKey(sequenceName, sequenceFactory);

        if(uninitializedSequencePrimaryKeyGenerators.containsKey(key))
        {
            SimulatedSequenceInitValues existing = uninitializedSequencePrimaryKeyGenerators.get(key);

            if(!existing.getPrimaryKeyAttributes().contains(primaryKeyAttribute))
            {
               validateInitValues(existing, initValues);
               existing.getPrimaryKeyAttributes().add(primaryKeyAttribute);
            }
        }
        else
        {
            initValues.getPrimaryKeyAttributes().add(primaryKeyAttribute);
            uninitializedSequencePrimaryKeyGenerators.put(key, initValues);
        }
    }

    private void validateInitValues(SimulatedSequenceInitValues existingValues, SimulatedSequenceInitValues newValues)
            throws MithraException
    {
        List<String> mithraInitializationErrors = new ArrayList<String>();

        if(existingValues.getIncrementSize() != newValues.getIncrementSize())
        {
            mithraInitializationErrors.add("The value for incrementSize must match the originalValue used: "+existingValues.getIncrementSize());
            logger.error("The value for incrementSize must match the originalValue used: "+existingValues.getIncrementSize());
        }

        if(existingValues.getInitialValue() != newValues.getInitialValue())
        {
             mithraInitializationErrors.add("The value for initialValue must match the originalValue used: "+existingValues.getInitialValue());
            logger.error("The value for initialValue must match the originalValue used: "+existingValues.getInitialValue());
        }

        if(existingValues.getSourceAttributeType() != newValues.getSourceAttributeType())
        {
            mithraInitializationErrors.add("Mithra objects sharing a simulated sequence must have the same source attribute type");
            logger.error("Mithra objects sharing a simulated sequence must have the same source attribute type");        
        }

        if(newValues.getIncrementSize() == 0)
        {
            mithraInitializationErrors.add("The incrementSize for a SimulatedSequence can not be 0");
            logger.error("The incrementSize for a SimulatedSequence can not be 0");
        }

        if(newValues.getBatchSize() < 0)
        {
            mithraInitializationErrors.add("The batchSize for a SimulatedSequence can not be less than 0");
            logger.error("The batchSize for a SimulatedSequence can not be less than 0");  
        }

        if(!mithraInitializationErrors.isEmpty())
        {
            for(int i = 0; i < mithraInitializationErrors.size(); i++)
            {
                logger.error(mithraInitializationErrors.get(i));
            }
            throw new MithraException("Could not initialize MithraSequences");
        }
    }

    public synchronized SimulatedSequencePrimaryKeyGenerator getSimulatedSequencePrimaryKeyGenerator(String sequenceName, String sequenceFactoryClassName, Object sourceAttribute)
    {
        SimulatedSequencePrimaryKeyGenerator simulatedSequencePrimaryKeyGenerator;

        CustomKey primaryKeyGeneratorKey = new CustomKey(sequenceName, sequenceFactoryClassName, sourceAttribute);

        simulatedSequencePrimaryKeyGenerator = simulatedSequencePrimaryKeyGenerators.get(primaryKeyGeneratorKey);

        if(simulatedSequencePrimaryKeyGenerator == null)
        {
            CustomKey key = new CustomKey(sequenceName, sequenceFactoryClassName);
            SimulatedSequenceInitValues initValues = uninitializedSequencePrimaryKeyGenerators.get(key);
            simulatedSequencePrimaryKeyGenerator = new SimulatedSequencePrimaryKeyGenerator(initValues);
            simulatedSequencePrimaryKeyGenerators.put(primaryKeyGeneratorKey, simulatedSequencePrimaryKeyGenerator);
        }
        return simulatedSequencePrimaryKeyGenerator;
    }

    public synchronized SimulatedSequencePrimaryKeyGenerator getSimulatedSequencePrimaryKeyGeneratorForNoSourceAttribute(String sequenceName, String sequenceFactoryClassName, Object sourceAttribute)
    {
        SimulatedSequencePrimaryKeyGenerator simulatedSequencePrimaryKeyGenerator;

        CustomKey primaryKeyGeneratorKey = new CustomKey(sequenceName, sequenceFactoryClassName);

        simulatedSequencePrimaryKeyGenerator = simulatedSequencePrimaryKeyGenerators.get(primaryKeyGeneratorKey);

        if(simulatedSequencePrimaryKeyGenerator == null)
        {
            SimulatedSequenceInitValues initValues = uninitializedSequencePrimaryKeyGenerators.get(primaryKeyGeneratorKey);
            simulatedSequencePrimaryKeyGenerator = new SimulatedSequencePrimaryKeyGenerator(initValues);
            simulatedSequencePrimaryKeyGenerators.put(primaryKeyGeneratorKey, simulatedSequencePrimaryKeyGenerator);
        }
        return simulatedSequencePrimaryKeyGenerator;
    }

    public synchronized MaxFromTablePrimaryKeyGenerator getMaxFromTablePrimaryKeyGenerator(Attribute primaryKeyAttribute, Object sourceAttribute)
    {
        MaxFromTablePrimaryKeyGenerator maxFromTablePrimaryKeyGenerator;

        CustomKey key = new CustomKey(primaryKeyAttribute.getClass().getName(), sourceAttribute);

        if(maxFromTablePrimaryKeyGenerators.containsKey(key))
        {
            maxFromTablePrimaryKeyGenerator = maxFromTablePrimaryKeyGenerators.get(key);
        }
        else
        {
            maxFromTablePrimaryKeyGenerator = new MaxFromTablePrimaryKeyGenerator(primaryKeyAttribute, sourceAttribute);
            maxFromTablePrimaryKeyGenerators.put(key, maxFromTablePrimaryKeyGenerator);
        }
        return maxFromTablePrimaryKeyGenerator;
    }

    public void clearPrimaryKeyGenerators()
    {
        if(simulatedSequencePrimaryKeyGenerators != null)
        {
            simulatedSequencePrimaryKeyGenerators.clear();
        }
        if(maxFromTablePrimaryKeyGenerators != null)
        {
            maxFromTablePrimaryKeyGenerators.clear();
        }
    }

    private static class CustomKey
    {
        String sequenceName;
        String className;
        Object sourceAttribute;

        public CustomKey(String sequenceName, String className, Object sourceAttribute)
        {
            this.sequenceName = sequenceName;
            this.className = className;
            this.sourceAttribute = sourceAttribute;
        }

        public CustomKey(String sequenceName, String className)
        {
            this(sequenceName, className, null);
        }

        public CustomKey(String className, Object sourceAttribute)
        {
            this(null, className, sourceAttribute);
        }

        public boolean equals(Object obj)
        {
            if(obj == this)
            {
                return true;
            }

            if(obj instanceof CustomKey)
            {
                CustomKey ck = (CustomKey)obj;
                return (sequenceName == null ? ck.sequenceName == null:sequenceName.equals(ck.sequenceName)) && className.equals(ck.className) &&
                      (sourceAttribute == null ? ck.sourceAttribute == null : sourceAttribute.equals(ck.sourceAttribute));
            }
            return false;
        }

        public int hashCode()
        {
            int result = 37;
            result = result*19 + (sequenceName != null ? sequenceName.hashCode() : 0);
            result = result*19 + className.hashCode();
            result = result*19 + (sourceAttribute != null ? sourceAttribute.hashCode() : 0);
            return result;
        }
    }
}

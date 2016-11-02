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

package com.gs.fw.common.mithra.util.serializer;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.util.Time;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class ReladomoSerializationContext
{
    public static final String RELADOMO_PREFIX = "_rdo";
    public static final String RELADOMO_META_DATA = RELADOMO_PREFIX + "MetaData";
    public static final String RELADOMO_CLASS_NAME = RELADOMO_PREFIX + "ClassName";
    public static final String RELADOMO_STATE = RELADOMO_PREFIX + "State";
    public static final String RELADOMO_LIST_SIZE = RELADOMO_PREFIX + "ListSize";

    public static final byte READ_ONLY_STATE = 10;
    public static final byte DETACHED_STATE = 20;
    public static final byte IN_MEMORY_STATE = 30;
    public static final byte DELETED_OR_TERMINATED_STATE = 40;

    protected final SerializationConfig serializationConfig;
    protected final SerialWriter writer;
    protected Object currentObjectBeingSerialized;

    protected SerializationNode currentNode;

    public ReladomoSerializationContext(SerializationConfig serializationConfig, SerialWriter writer)
    {
        this.serializationConfig = serializationConfig;
        this.currentNode = this.serializationConfig.getRootNode();
        this.writer = writer;
    }

    public void serializeReladomoObject(MithraObject reladomoObject)
    {
        try
        {
            writer.startReladomoObject(reladomoObject, this);
            Object previousObject = this.currentObjectBeingSerialized;
            this.currentObjectBeingSerialized = reladomoObject;

            if (this.serializationConfig.serializeMetaData())
            {
                writer.startMetadata(reladomoObject, this);
                serializeClassMetadata(reladomoObject);
                writer.writeMetadataEnd(reladomoObject, this);
            }

            writer.startAttributes(this, this.currentNode.getAttributes().size());
            serializeAttributes(reladomoObject);
            writer.endAttributes(this);

            writer.startRelationships(this, this.currentNode.getChildren().size());
            serializeRelationships(reladomoObject);
            writer.endRelationships(this);

            writer.startLinks(this, this.currentNode.getLinks().size());
            serializeLinks(reladomoObject);
            writer.endLinks(this);

            List<Method> annotatedMethods = getAnnotatedMethods(reladomoObject);
            writer.startAnnotatedMethod(reladomoObject, this, annotatedMethods.size());
            serializeAnnotatedMethods(reladomoObject, annotatedMethods);
            writer.endAnnotatedMethod(reladomoObject, this);

            writer.endReladomoObject(reladomoObject, this);
            this.currentObjectBeingSerialized = previousObject;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not serialize object", e);
        }
    }

    protected List<Method> getAnnotatedMethods(Object reladomoObject)
    {
        return this.serializationConfig.getAnnotatedMethods(reladomoObject.getClass());
    }

    protected void serializeLinks(MithraObject reladomoObject) throws Exception
    {
        List<SerializationNode> links = this.currentNode.getLinks();
        for(int i=0;i<links.size();i++)
        {
            SerializationNode linkNode = links.get(i);
            writer.writeLink(this, linkNode.getRelatedFinder().getRelationshipName(), linkNode.getLinkAttributes());
        }
    }

    protected void serializeAnnotatedMethods(MithraObject reladomoObject, List<Method> annotatedMethods) throws Exception
    {
        for(int i=0;i<annotatedMethods.size();i++)
        {
            Method method = annotatedMethods.get(i);
            try
            {
                Object methodResult = method.invoke(reladomoObject, null);
                if (methodResult == null)
                {
                    writer.writeNull(this, getMethodAttributeName(method), method.getReturnType());
                }
                else
                {
                    writeByType(reladomoObject, methodResult, getMethodAttributeName(method));
                }
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException("Could not call method "+method.getName()+" on class "+reladomoObject.getClass().getName(), e);
            }
            catch (InvocationTargetException e)
            {
                throw new RuntimeException("Could not call method "+method.getName()+" on class "+reladomoObject.getClass().getName(), e);
            }
        }
    }

    protected void serializeAnnotatedListMethods(Object list, List<Method> annotatedMethods) throws Exception
    {
        for(int i=0;i<annotatedMethods.size();i++)
        {
            Method method = annotatedMethods.get(i);
            try
            {
                Object methodResult = method.invoke(list, null);
                if (methodResult == null)
                {
                    writer.writeNull(this, getMethodAttributeName(method), method.getReturnType());
                }
                else
                {
                    writeByType(null, methodResult, getMethodAttributeName(method));
                }
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException("Could not call method "+method.getName()+" on class "+list.getClass().getName(), e);
            }
            catch (InvocationTargetException e)
            {
                throw new RuntimeException("Could not call method "+method.getName()+" on class "+list.getClass().getName(), e);
            }
        }
    }

    protected void writeByType(MithraObject reladomoObject, Object methodResult, String methodAttributeName) throws Exception
    {
        if (methodResult instanceof Boolean)
        {
            writer.writeBoolean(this, methodAttributeName, ((Boolean) methodResult));
        }
        else if (methodResult instanceof Byte)
        {
            writer.writeByte(this, methodAttributeName, ((Byte) methodResult));
        }
        else if (methodResult instanceof Short)
        {
            writer.writeShort(this, methodAttributeName, ((Short) methodResult));
        }
        else if (methodResult instanceof Integer)
        {
            writer.writeInt(this, methodAttributeName, ((Integer) methodResult));
        }
        else if (methodResult instanceof Long)
        {
            writer.writeByte(this, methodAttributeName, ((Byte) methodResult));
        }
        else if (methodResult instanceof Character)
        {
            writer.writeChar(this, methodAttributeName, ((Character) methodResult));
        }
        else if (methodResult instanceof Float)
        {
            writer.writeFloat(this, methodAttributeName, ((Float) methodResult));
        }
        else if (methodResult instanceof Double)
        {
            writer.writeDouble(this, methodAttributeName, ((Double) methodResult));
        }
        else if (methodResult instanceof byte[])
        {
            writer.writeByteArray(this, methodAttributeName, ((byte[]) methodResult));
        }
        else if (methodResult instanceof BigDecimal)
        {
            writer.writeBigDecimal(this, methodAttributeName, ((BigDecimal) methodResult));
        }
        else if (methodResult instanceof Timestamp)
        {
            writer.writeTimestamp(this, methodAttributeName, ((Timestamp) methodResult));
        }
        else if (methodResult instanceof Date)
        {
            writer.writeDate(this, methodAttributeName, ((Date) methodResult));
        }
        else if (methodResult instanceof String)
        {
            writer.writeString(this, methodAttributeName, ((String) methodResult));
        }
        else if (methodResult instanceof Time)
        {
            writer.writeTime(this, methodAttributeName, ((Time) methodResult));
        }
        else
        {
            writer.writeObject(this, methodAttributeName, methodResult);
        }
    }

    private String getMethodAttributeName(Method method)
    {
        String name = method.getName();
        if (name.startsWith("get") && name.length() > 4 && Character.isUpperCase(name.charAt(3)))
        {
            return Character.toLowerCase(name.charAt(3))+name.substring(4);
        }
        return name;
    }

    protected void serializeRelationships(MithraObject reladomoObject) throws Exception
    {
        SerializationNode pushedNode = this.currentNode;
        List<SerializationNode> children = this.currentNode.getChildren();
        for(int i=0;i<children.size();i++)
        {
            SerializationNode serializationNode = children.get(i);
            AbstractRelatedFinder relatedFinder = serializationNode.getRelatedFinder();
            this.currentNode = serializationNode;
            if (relatedFinder.isToOne())
            {
                MithraObject related = (MithraObject) relatedFinder.valueOf(reladomoObject);
                if (related == null)
                {
                    this.writer.writeNull(this, relatedFinder.getRelationshipName(), serializationNode.getRelatedClass());
                }
                else
                {
                    this.writer.startRelatedObject(this, relatedFinder.getRelationshipName(), relatedFinder, related);
                    this.serializeReladomoObject(related);
                    this.writer.endRelatedObject(this, relatedFinder.getRelationshipName(), relatedFinder, related);
                }
            }
            else
            {
                MithraList relatedList = (MithraList) relatedFinder.valueOf(reladomoObject);
                this.writer.startRelatedReladomoList(this, relatedFinder.getRelationshipName(), relatedFinder, relatedList);
                this.serializeListComponents(relatedList);
                this.writer.endRelatedReladomoList(this, relatedFinder.getRelationshipName(), relatedFinder, relatedList);
            }
        }
        this.currentNode = pushedNode;
    }

    protected void serializeAttributes(MithraObject reladomoObject) throws Exception
    {
        List<Attribute> attributes = this.currentNode.getAttributes();
        for(int i=0;i<attributes.size();i++)
        {
            attributes.get(i).zWriteSerial(this, this.writer, reladomoObject);
        }
    }

    protected void serializeClassMetadata(MithraObject reladomoObject) throws Exception
    {
        writer.writeString(this, RELADOMO_CLASS_NAME, reladomoObject.getClass().getName());
        byte state;
        if (reladomoObject instanceof MithraTransactionalObject)
        {
            MithraTransactionalObject txObj = (MithraTransactionalObject) reladomoObject;
            if (txObj.zIsDetached())
            {
                state = DETACHED_STATE;
            }
            else if (txObj.isInMemoryAndNotInserted() || txObj.isInMemoryNonTransactional())
            {
                state = IN_MEMORY_STATE;
            }
            else
            {
                state = DETACHED_STATE; // the object is in persisted state, but we're serializing as detached.
            }
        }
        else
        {
            state = READ_ONLY_STATE;
        }
        writer.writeByte(this, RELADOMO_STATE, state);
    }

    public void serializeReladomoList(MithraList reladomoList)
    {
        try
        {
            writer.startReladomoList(reladomoList, this);

            serializeListComponents(reladomoList);

            writer.endReladomoList(reladomoList, this);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not serialize list", e);
        }
    }

    protected void serializeListComponents(MithraList reladomoList) throws Exception
    {
        Object previousList = this.currentObjectBeingSerialized;
        this.currentObjectBeingSerialized = reladomoList;
        if (this.serializationConfig.serializeMetaData())
        {
            writer.startReladomoListMetatdata(reladomoList, this);
            writer.writeString(this, RELADOMO_CLASS_NAME, getCurrentClassName());
            writer.writeInt(this, RELADOMO_LIST_SIZE, reladomoList.size());
            writer.endReladomoListMedatadata(reladomoList, this);
        }

        List<Method> annotatedMethods = getAnnotatedMethods(reladomoList);
        writer.startListAnnotatedMethods(reladomoList, this, annotatedMethods.size());
        serializeAnnotatedListMethods(reladomoList, annotatedMethods);
        writer.endListAnnotatedMethods(reladomoList, this);

        writer.startReladomoListElements(reladomoList, this);
        for(int i=0;i<reladomoList.size();i++)
        {
            MithraObject reladomoObject = (MithraObject) reladomoList.get(i);
            writer.startReladomoListItem(reladomoList, this, i, reladomoObject);
            serializeReladomoObject(reladomoObject);
            writer.endReladomoListItem(reladomoList, this, i, reladomoObject);
        }
        writer.endReladomoListElements(reladomoList, this);
        this.currentObjectBeingSerialized = previousList;
    }

    protected String getCurrentClassName()
    {
        String finderClassName = this.currentNode.getRelatedFinder().getFinderClassName();
        return finderClassName.substring(0, finderClassName.length() - "Finder".length());
    }

    public Object getCurrentObjectBeingSerialized()
    {
        return currentObjectBeingSerialized;
    }
}

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

package com.gs.fw.common.mithra.generator;

import com.gs.fw.common.mithra.generator.metamodel.IndexType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.ArrayList;
import java.util.List;


public class Index
{
    private Attribute[] attributes;
    private String[] unresolvedAttributes;
    private String name;
    private boolean unique;
    private boolean pkStatus = false;
    private boolean isSameAsPk;
    private Index sameIndex = null;
    private MithraObjectTypeWrapper wrapper;

    public Index(Attribute[] attributes, String name, boolean unique, MithraObjectTypeWrapper wrapper)
    {
        this.attributes = attributes;
        this.name = name;
        this.unique = unique;
        this.wrapper = wrapper;
    }

    public void setPkStatus(boolean pkStatus)
    {
        this.pkStatus = pkStatus;
    }

    public boolean isPk()
    {
        return pkStatus;
    }

    public Index(IndexType indexType, MithraObjectTypeWrapper wrapper)
    {
        this.name = indexType.getName();
        this.unique = indexType.isUnique();
        this.wrapper = wrapper;
        String[] attributeNames = indexType.value().split(", *");
        ArrayList good = new ArrayList();
        ArrayList bad = new ArrayList();
        for (int i = 0; i < attributeNames.length; i++)
        {
            if (attributeNames[i].length() > 0)
            {
                Attribute a = wrapper.getAttributeByName( attributeNames[i].trim() );
                if (a == null)
                {
                    bad.add(attributeNames[i]);
                }
                else good.add(a);
            }
        }
        if (wrapper.hasSourceAttribute() && this.isUnique())
        {
            good.add(wrapper.getSourceAttribute());
        }
        this.attributes = new Attribute[ good.size() ];
        good.toArray(this.attributes);
        this.unresolvedAttributes = new String[bad.size()];
        bad.toArray(this.unresolvedAttributes);
    }

    public AbstractAttribute[] getAttributes()
    {
        return attributes;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getIndexColumns()
    {
        String result = "";
        for(int i=0;i<attributes.length;i++)
        {
            if (!attributes[i].isSourceAttribute())
            {
                if (result.length() > 0) result += ", ";
                result += attributes[i].getColumnName();
            }
        }
        return result;
    }

    public ArrayList<String> getIndexColumnsNames()
    {
        ArrayList<String> names = new ArrayList<String>();

        for (int i = 0; i < attributes.length; i++)
        {
            if (!attributes[i].isSourceAttribute())
            {
                names.add(attributes[i].getColumnName());
            }
        }
        return names;
    }

    public boolean isUnique()
    {
        return unique;
    }

    public void checkConsistency(List<String> errors)
    {
        for(int i=0;i<unresolvedAttributes.length;i++)
        {
            errors.add("Could not resolve attribute "+unresolvedAttributes[i]+" for index "+this.name);
        }
    }

    public boolean isRedundantIndex(List attributes)
    {
        List copy = new ArrayList(attributes);
        for(int i=0;i<this.attributes.length;i++)
        {
            copy.remove(this.attributes[i]);
        }
        if (copy.size() == 0 && attributes.size() == this.attributes.length) return true;
        if (this.isUnique())
        {
            if(this.attributes.length != attributes.size())
            {
                return false;
            }
            for(int i=0;i<this.attributes.length;i++)
            {
                if (!attributes.contains(this.attributes[i]))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public boolean hasFastPathLookup()
    {
        BeanState beanState = new BeanState();

        for(int i=0;i<attributes.length;i++)
        {
            beanState.increment(attributes[i]);
        }
        return beanState.getIntCount() <= 4 && beanState.getLongCount() <= 4 && beanState.getObjectCount() <= 4;
    }

    public String getSanitizedUpperCaseName()
    {
        String result = StringUtility.makeIntoJavaIdentifier(this.name);
        if (result.startsWith("by") && result.length() > 2)
        {
            result = result.substring(2);
        }
        return StringUtility.firstLetterToUpper(result);
    }

    public String getFindByParameters()
    {
        String result = attributes[0].getType().getJavaTypeString() + " "+attributes[0].getName();
        for(int i=1;i<attributes.length;i++)
        {
            result += ", "+attributes[i].getType().getJavaTypeString() + " "+attributes[i].getName();
        }
        if (wrapper.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
            for(AsOfAttribute attr: asOfAttributes)
            {
                result += ", Timestamp "+attr.getName();
            }
        }
        return result;
    }

    public String getFindByVariables()
    {
        String result = attributes[0].getName();
        for(int i=1;i<attributes.length;i++)
        {
            result += ", "+attributes[i].getName();
        }
        if (wrapper.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
            for(AsOfAttribute attr: asOfAttributes)
            {
                result += ", "+attr.getName();
            }
        }
        return result;
    }

    public String getBeanType()
    {
        return "I3O3L3";
    }

    public String getLookupEqualsCondition()
    {
        BeanState beanState = new BeanState();

        String result;
        String beanAttribute = "_bean."+attributes[0].getBeanGetter(beanState.getIntCount(), beanState.getLongCount(), beanState.getObjectCount())+"()";
        if (attributes[0].getType().isPrimitive())
        {
            result = beanAttribute+" == _castedTargetData."+attributes[0].getGetter()+"()";
        }
        else if (attributes[0].isArray())
        {
            result = "Arrays.equals("+beanAttribute+", _castedTargetData."+attributes[0].getGetter()+"())";
        }
        else
        {
            result = beanAttribute + ".equals(_castedTargetData."+attributes[0].getGetter()+"())";
        }
        beanState.increment(attributes[0]);
        for(int i=1;i<attributes.length;i++)
        {
            beanAttribute = "_bean."+attributes[i].getBeanGetter(beanState.getIntCount(), beanState.getLongCount(), beanState.getObjectCount())+"()";
            if (attributes[i].getType().isPrimitive())
            {
                result += " && "+beanAttribute +" == _castedTargetData." + attributes[i].getGetter() + "()";
            }
            else if (attributes[i].isArray())
            {
                result += " && Arrays.equals("+beanAttribute+", _castedTargetData."+attributes[i].getGetter()+"())";
            }
            else
            {
                result += " && "+beanAttribute+".equals(_castedTargetData."+attributes[i].getGetter()+"())";
            }
            beanState.increment(attributes[i]);
        }
        return result;
    }

    public String getAsOfAttributeCheckCondition()
    {
        if (wrapper.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
            String result = wrapper.getFinderClassName()+"."+asOfAttributes[0].getName()+"().dataMatches(_castedTargetData, _asOfDate0)";
            if (asOfAttributes.length == 2)
            {
                result += " && "+ wrapper.getFinderClassName()+"."+asOfAttributes[1].getName()+"().dataMatches(_castedTargetData, _asOfDate1)";
            }
            return result;
        }
        else
        {
            return "true";
        }
    }

    public String getLookupHashCompute(boolean offHeap)
    {
        BeanState beanState = new BeanState();

        String result = addHashCodeExpression(null, attributes[0], beanState, offHeap);
        beanState.increment(attributes[0]);
        for(int i=1;i<attributes.length;i++)
        {
            result = addHashCodeExpression(result, attributes[i], beanState, offHeap);
            beanState.increment(attributes[i]);
        }
        return result;
    }

    private String addHashCodeExpression(String result, AbstractAttribute uniqueAttribute, BeanState beanState, boolean offHeap)
    {
        boolean combine = false;
        String hashMethod = "hash";
        if (uniqueAttribute.isStringAttribute() && offHeap)
        {
            hashMethod = "offHeapHash";
        }
        if (result != null)
        {
            combine = true;
            result = "HashUtil.combineHashes("+result+",HashUtil."+hashMethod+"(";
        }
        else
        {
            result = "HashUtil."+hashMethod+"(";
        }
        result += "_bean."+uniqueAttribute.getBeanGetter(beanState.getIntCount(), beanState.getLongCount(), beanState.getObjectCount());
        result += "())";
        if (combine) result += ")";
        return result;
    }

    public String getLookupNotNullCheck()
    {
        String result = "";
        for(int i=0;i<attributes.length;i++)
        {
            if (!attributes[i].isPrimitive())
            {
                if (result.length() > 0) result += " && ";
                result += attributes[i].getName()+" != null";
            }
        }
        if (result.length() > 0)
        {
            result = "if ("+result+")";
        }
        return result;
    }

    public String getLookupBeanSetters()
    {
        String result = "";
        BeanState beanState = new BeanState();
        for(int i=0;i<attributes.length;i++)
        {
            result += "\n";
            result += "_bean."+attributes[i].getBeanSetter(beanState)+"("+attributes[i].getName()+");";
            beanState.increment(attributes[i]);
        }
        return result;
    }

    public String getLookupCacheParameters()
    {
        String result = "_bean, _bean, for"+this.getSanitizedUpperCaseName()+", ";
        if (wrapper.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
            result += asOfAttributes[0].getName();
            result += ", ";
            if (asOfAttributes.length == 2)
            {
                result += asOfAttributes[1].getName();
            }
            else
            {
                result += "null";
            }
        }
        else
        {
            result += "null, null";
        }
        if (!isPk())
        {
            result += ", "+wrapper.getFinderClassName()+".zGetIndex"+this.getName()+"Ref()";
        }
        return result;
    }

    public String getLookupOperation()
    {
        String result = "this."+attributes[0].getName()+"().eq("+attributes[0].getName()+")";
        for(int i=1;i<attributes.length;i++)
        {
            result += ".and(this."+attributes[i].getName()+"().eq("+attributes[i].getName()+"))";
        }
        if (wrapper.hasAsOfAttributes())
        {
            AsOfAttribute[] asOfAttributes = wrapper.getAsOfAttributes();
            for(AsOfAttribute attr: asOfAttributes)
            {
                result += ".and(this."+attr.getName()+"().eq("+attr.getName()+"))";
            }
        }
        return result;
    }

    public String getCacheLookupMethod()
    {
        if (this.isPk())
        {
            return "getAsOneFromCache";
        }
        return "getAsOneByIndexFromCache";
    }

    public boolean isSameAsPk()
    {
        return this.isSameAsPk;
    }

    public void setIsSameAsPk(boolean isSameAsPk)
    {
        this.isSameAsPk = isSameAsPk;
    }

    public void setSameIndex(Index index)
    {
        this.sameIndex = index;
    }

    public Index getSameIndex()
    {
        return this.sameIndex;
    }
}

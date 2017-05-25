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

import com.gs.fw.common.mithra.generator.metamodel.MithraBaseObjectType;
import com.gs.fw.common.mithra.generator.metamodel.SuperClassAttributeType;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.List;


public abstract class MithraBaseObjectTypeWrapper implements CommonWrapper
{
    private static final String IS_NULL_BITS = "isNullBits";

    private MithraBaseObjectType wrapped;
    private MithraBaseObjectTypeWrapper superClassWrapper;
    private String sourceFileName;
    private String importedSource;

    protected String abstractClassName;
    protected String dataClassName;
    protected String finderClassName;
    protected String dbAbstractClassName;
    protected String dbClassName;
    protected String listAbstractClassName;
    protected String listClassName;
    protected String interfaceName;
    protected String abstractInterfaceName;
    protected String listInterfaceName;
    protected String listAbstractInterfaceName;
    protected String implClassName;

    private String uniqueAlias;
    private NullBitsHolder[] nullBitsHolders;
    private boolean ignoreNonGeneratedAbstractClasses = false;
    private boolean generateInterfaces = false;
    private boolean readOnlyInterfaces = false;
    private boolean generateFileHeaders = false;


    public MithraBaseObjectTypeWrapper(MithraBaseObjectType wrapped, String sourceFileName, String importedSource)
    {
        this.wrapped = wrapped;
        this.sourceFileName = sourceFileName;
        this.importedSource = importedSource;
    }

    public boolean isGenerateFileHeaders()
    {
        return generateFileHeaders;
    }

    public void setGenerateFileHeaders(boolean generateFileHeaders)
    {
        this.generateFileHeaders = generateFileHeaders;
    }

    public MithraBaseObjectType getWrapped()
    {
        return this.wrapped;
    }

    public MithraBaseObjectTypeWrapper getSuperClassWrapper()
    {
        return this.superClassWrapper;
    }

    public void setSuperClassWrapper(MithraBaseObjectTypeWrapper superClassWrapper)
    {
        this.superClassWrapper = superClassWrapper;
    }

    public String getClassName()
    {
        return this.wrapped.getClassName();
    }

    public String getInterfaceName()
    {
        return this.interfaceName;
    }

    public String getPackageName()
    {
        return this.wrapped.getPackageName();
    }

    public SuperClassAttributeType getSuperClass()
    {
        return this.wrapped.getSuperClass();
    }

    public List<String> getExtraImports()
    {
        return this.wrapped.getImports();
    }

    public String getSourceFileName()
    {
        return this.sourceFileName;
    }

    public boolean isImported()
    {
        return this.importedSource != null;
    }

    public String getImportedSource()
    {
        return this.importedSource;
    }

    public String getImplClassName()
    {
        return implClassName;
    }

    public String getAbstractClassName()
    {
        return this.abstractClassName;
    }

    public String getAbstractInterfaceName()
    {
        return this.abstractInterfaceName;
    }

    public String getDataClassName()
    {
        return this.dataClassName;
    }

    public String getFinderClassName()
    {
        return this.finderClassName;
    }

    public String getDbAbstractClassName()
    {
        return this.dbAbstractClassName;
    }

    public String getDbClassName()
    {
        return this.dbClassName;
    }

    public String getListAbstractClassName()
    {
        return this.listAbstractClassName;
    }

    public String getListAbstractInterfaceName()
    {
        return this.listAbstractInterfaceName;
    }


    public String getListClassName()
    {
        return this.listClassName;
    }

    public String getListInterfaceName()
    {
        return this.listInterfaceName;
    }

    public String getFullyQualifiedSuperClassType()
    {
        String result = null;
        if (this.getSuperClass() != null)
        {
            result = this.getSuperClass().getName();
            if (this.getSuperClassWrapper() != null)
            {
                result = this.getSuperClassWrapper().getPackageName() + "." + this.getSuperClassWrapper().getImplClassName();
            }
            else if (this.ignoreNonGeneratedAbstractClasses)
            {
                result = null;
            }
        }
        return result;
    }

    public String getFullyQualifiedSuperClassInterface()
    {
        String result = null;
        if (this.getSuperClass() != null)
        {
            result = this.getSuperClass().getName();
            if (this.getSuperClassWrapper() != null)
            {
                result = this.getSuperClassWrapper().getPackageName() + "." + this.getSuperClassWrapper().getClassName();
            }
        }
        return result;
    }

    public NullBitsHolder[] getNullBitsHolders()
    {
        return this.nullBitsHolders;
    }

    public void setNullBitsHolders(NullBitsHolder[] nullBitsHolders)
    {
        this.nullBitsHolders = nullBitsHolders;
    }

    public String getNullGetterExpressionForIndex(int index)
    {
        int attributeIndex = index / 64;
        return "(" + this.nullBitsHolders[attributeIndex].getName() + " & " + this.getNullBitMask(index) + ") != 0 ";
    }

    public String getNullSetterExpressionForIndex(int index)
    {
        int attributeIndex = index / 64;
        NullBitsHolder nullBitsHolder = this.nullBitsHolders[attributeIndex];
        String mask = this.getNullBitMask(index);
        if (nullBitsHolder.getType().equals("byte") || nullBitsHolder.getType().equals("short"))
        {
            return nullBitsHolder.getName() + " = (" + nullBitsHolder.getType() + ")((int)" + nullBitsHolder.getName() + " | "+ mask + ")";
        }
        else
        {
            return nullBitsHolder.getName() + " = (" + nullBitsHolder.getName() + " | " + mask + ")";
        }
    }

    public String getNotNullSetterExpressionForIndex(int index)
    {
        int attributeIndex = index / 64;
        NullBitsHolder nullBitsHolder = this.nullBitsHolders[attributeIndex];
        String mask = " ~( " + this.getNullBitMask(index)+ ")";
        if (nullBitsHolder.getType().equals("byte") || nullBitsHolder.getType().equals("short"))
        {
            return nullBitsHolder.getName() + " = (" + nullBitsHolder.getType() + ")((int)" + nullBitsHolder.getName() + " & " + mask + ")";
        }
        else
        {
            return nullBitsHolder.getName() + " = " + nullBitsHolder.getName() + " & " + mask;
        }
    }

    protected String getNullBitMask(int index)
    {
        int indexMod64 = index % 64;
        if (indexMod64 == 0)
        {
            return "1";
        }
        String mask = "1 << " + indexMod64;
        if (indexMod64 >= 31)
        {
            mask = "1L << " + indexMod64;
        }
        return mask;
    }

    protected void initializeNullBitHolders(int count)
    {
        if (count != 0)
        {
            int numAttributes = (int) Math.ceil(count / 64.0);
            this.nullBitsHolders = new NullBitsHolder[numAttributes];
            int remainder = count;
            for (int i = 0; i < numAttributes; i++)
            {
                int bits = Math.min(remainder, 64);
                String initalValue = "0";
                if (this.wrapped.isInitializePrimitivesToNull())
                {
                    long result = 0;
                    for(int j=0;j<bits;j++)
                    {
                        result |= (1L << j);
                    }
                    initalValue = "("+this.getTypeForSize(bits)+") "+result+"L"; 
                }
                this.nullBitsHolders[i] = new NullBitsHolder(this.getTypeForSize(bits), IS_NULL_BITS + i, initalValue);
                remainder -= bits;
            }
        }
    }

    protected String getTypeForSize(int count)
    {
        String result = null;
        if (count <= 8)
        {
            result = "byte";
        }
        else if (count <= 16)
        {
            result = "short";
        }
        else if (count < 32)
        {
            result = "int";
        }
        else if (count <= 64)
        {
            result = "long";
        }
        return result;
    }

    public String getUniqueAlias()
    {
        return this.uniqueAlias;
    }

    public String getUniqueAliasAsString()
    {
        if (this.uniqueAlias == null)
        {
            return "null";
        }
        return '"' + this.uniqueAlias + '"';
    }

    public void setUniqueAlias(String uniqueAlias)
    {
        this.uniqueAlias = uniqueAlias;
    }

    public void setIgnoreNonGeneratedAbstractClasses(boolean ignoreNonGeneratedAbstractClasses)
    {
        this.ignoreNonGeneratedAbstractClasses = ignoreNonGeneratedAbstractClasses;
    }

    public abstract String getDescription();
    public abstract String getObjectType();
    public abstract boolean isTablePerSubclassSuperClass();

    protected void createAuxiliaryClassNames()
    {
        this.createAuxiliaryClassNames(false);
    }

    protected void createAuxiliaryClassNames(boolean isGenerateInterfaces)
    {
        String className = this.getClassName();
        this.dataClassName = className + "Data";
        this.finderClassName = className + "Finder";
        this.dbAbstractClassName = className + "DatabaseObjectAbstract";
        this.dbClassName = className + "DatabaseObject";
        if(isGenerateInterfaces)
        {
            this.listAbstractClassName = className + "ListImplAbstract";            
            this.listClassName = className + "ListImpl";
            this.abstractClassName = className + "ImplAbstract";
            this.implClassName = className + "Impl";
        }
        else
        {
            this.abstractClassName = className + "Abstract";
            this.listAbstractClassName = className + "ListAbstract";
            this.listClassName = className + "List";
            this.implClassName = className;
        }
        this.interfaceName = className;
        this.listInterfaceName = className + "List";
        this.abstractInterfaceName = className + "Abstract";
        this.listAbstractInterfaceName = className + "ListAbstract";
    }

    public boolean isPure()
    {
        return false;
    }

    public void setGenerateInterfaces(boolean generateInterfaces)
    {
        this.generateInterfaces = generateInterfaces;
    }

    public boolean isGenerateInterfaces()
    {
        return this.generateInterfaces;
    }

    public void setReadOnlyInterfaces(boolean readOnlyInterfaces)
    {
        this.readOnlyInterfaces = readOnlyInterfaces;
    }

    public boolean isReadOnlyInterfaces()
    {
        return this.readOnlyInterfaces;
    }
    
    public boolean hasSuperClass()
    {
        return this.getFullyQualifiedSuperClassType() != null;
    }

    public class NullBitsHolder
    {
        private final String type;
        private final String name;
        private final String initialValue;

        public NullBitsHolder(String type, String name, String initialValue)
        {
            this.type = type;
            this.name = name;
            this.initialValue = initialValue;
        }

        public String getType()
        {
            return this.type;
        }

        public String getName()
        {
            return this.name + (MithraBaseObjectTypeWrapper.this.getUniqueAlias() == null ? "" : MithraBaseObjectTypeWrapper.this.getUniqueAlias());
        }

        public String getIoType()
        {
            return StringUtility.firstLetterToUpper(this.type);
        }

        public String getInitialValue()
        {
            return initialValue;
        }
    }

    public MithraSuperTypeWrapper getSubstituteSuperType()
    {
        return null;
    }
}

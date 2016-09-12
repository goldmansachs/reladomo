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

import com.gs.fw.common.mithra.generator.type.JavaType;
import com.gs.fw.common.mithra.generator.type.JavaTypeException;
import com.gs.fw.common.mithra.generator.util.StringUtility;
import com.gs.fw.common.mithra.generator.metamodel.TransactionalMethodSignatureType;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;



public class TransactionalMethodSignature
{
    private TransactionalMethodSignatureType wrapped;

    public TransactionalMethodSignature(TransactionalMethodSignatureType wrapped)
    {
        this.wrapped = wrapped;
    }

    public boolean isParentImplements()
    {
        return this.wrapped.isParentImplements();
    }

    protected String getMethodExceptions()
    {
        StringTokenizer tokenizer = new StringTokenizer(getOriginalMethodSignature(), ")");
        tokenizer.nextToken();
        if (tokenizer.hasMoreTokens())
        {
            return tokenizer.nextToken().trim();
        }
        return "";
    }

    public String getOriginalMethodSignature()
    {
        return this.wrapped.value();
    }

    public List<String> getExceptions()
    {
        String tmp = this.getMethodExceptions();
        tmp = StringUtility.replaceStr(tmp, "throws", "");
        StringTokenizer tokenizer = new StringTokenizer(tmp, ",");
        List<String> exceptions = new ArrayList<String>();
        while (tokenizer.hasMoreTokens())
        {
            String exception = tokenizer.nextToken().trim();
            if (!exception.equals("MithraBusinessException"))
            {
                exceptions.add(exception);
            }
        }
        return exceptions;
    }

    protected String getMethodParameters()
    {
        StringTokenizer tokenizer = new StringTokenizer(this.getOriginalMethodSignature(), "(");
        tokenizer.nextToken();
        if (tokenizer.hasMoreTokens())
        {
            String result = tokenizer.nextToken(")").trim().substring(1);
            if(!result.equals("")) result = result + ",";
            return result;
        }
        return "";
    }

    public String getMethodParametersNoType()
    {
        String tmp = this.getMethodParameters();
        StringTokenizer tokenizer = new StringTokenizer(tmp, ",");
        String parameters = "";
        int noOfArguments = tokenizer.countTokens();
        if(noOfArguments > 0)
        {
            while (tokenizer.hasMoreTokens())
            {
                String param = tokenizer.nextToken().trim();
                parameters += param.substring(param.indexOf(" ")).trim() + ",";
            }
        }
        return parameters;
    }

    public String getMethodName()
    {
        StringTokenizer tokenizer = new StringTokenizer(this.getOriginalMethodSignature(), "(");
        String tmp = tokenizer.nextToken();
        return tmp.substring(tmp.lastIndexOf(" ")).trim();
    }

    public String getReturnType()
    {
        StringTokenizer tokenizer = new StringTokenizer(this.getOriginalMethodSignature(), "(");
        String tmp = tokenizer.nextToken();
        tmp = StringUtility.replaceStr(tmp, this.getMethodName(), "");
        tmp = StringUtility.replaceStr(tmp, "public", "");
        tmp = StringUtility.replaceStr(tmp, "private", "");
        tmp = StringUtility.replaceStr(tmp, "protected", "");
        tmp = StringUtility.replaceStr(tmp, "final", "");
        return tmp.trim();
    }

    public boolean isVoid()
    {
        return getReturnType().equals("void");
    }

    protected String getMethodScope()
    {
        StringTokenizer tokenizer = new StringTokenizer(this.getOriginalMethodSignature(), "(");
        String tmp = tokenizer.nextToken();
        tmp = StringUtility.replaceStr(tmp, getMethodName(), "");
        tmp = StringUtility.replaceStr(tmp, getReturnType(), "");
        return tmp.trim();
    }

    public String getImplMethodSignature()
    {
        if (this.wrapped.isParentImplements()) return "";
        String result = "protected abstract ";
        result += this.getReturnType()+" ";
        result += this.getMethodName()+"Impl";
        result += "("+this.getMethodParameters()+" MithraTransaction mithraTransaction)";
        result += this.getMethodExceptions();
        result += ";";
        return result;
    }

    public String getMethodSignatureWithRetryCount()
    {
        String result = "protected ";
        result +=  this.getReturnType()+" ";
        result += this.getMethodName();
        result += "("+this.getMethodParameters()+" int _retryCount)";
        result += this.getMethodExceptions();
        return result;
    }

    public String getDefaultInitialValueForReturnType()
    {
        try
        {
            JavaType type = JavaType.create(this.getReturnType());
            return type.getDefaultInitialValue();
        }
        catch (JavaTypeException e)
        {
            return "null";
        }
    }
}

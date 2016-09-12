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

package com.gs.fw.common.mithra.generator.computedattribute;


import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.computedattribute.functiongen.FunctionGenerator;
import com.gs.fw.common.mithra.generator.computedattribute.type.Type;
import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FunctionExpression extends Expression
{
    private String functionName;
    private Expression sourceExpression;
    private List<Expression> parameters = new ArrayList<Expression>();
    private boolean finished;
    private FunctionGenerator functionGenerator;

    public FunctionExpression(Expression expression)
    {
        this.sourceExpression = expression;
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public void setFunctionName(String functionName)
    {
        this.functionName = functionName;
    }

    @Override
    public Type getType()
    {
        return this.functionGenerator.getReturnType();
    }

    private FunctionGenerator createFunctionGenerator(List<String> errors)
    {
        try
        {
            String className = "com.gs.fw.common.mithra.generator.functiongen"+
                    sourceExpression.getType().toString() + StringUtility.firstLetterToUpper(this.functionName) + "FuncGen";
            return (FunctionGenerator) Class.forName(className).newInstance();
        }
        catch (Exception e)
        {
            errors.add("Could not find function with name "+this.functionName+"for type "+sourceExpression.getType());
        }
        return null;
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public void setParameters(List<Expression> parameters)
    {
        this.parameters = parameters;
    }

    public Expression getSourceExpression()
    {
        return sourceExpression;
    }

    public void addParameter(Expression parameter)
    {
        parameters.add(parameter);
    }

    public void markFinished()
    {
        this.finished = true;
    }

    public boolean isFinished()
    {
        return this.finished;
    }

    @Override
    public void addAttributeList(Set<String> result)
    {
        this.sourceExpression.addAttributeList(result);
        for(int i=0;i<parameters.size();i++)
        {
            parameters.get(i).addAttributeList(result);
        }
    }

    @Override
    public void resolveAttributes(MithraObjectTypeWrapper wrapper, List<String> errors)
    {
        this.sourceExpression.resolveAttributes(wrapper, errors);
        for(int i=0;i<parameters.size();i++)
        {
            parameters.get(i).resolveAttributes(wrapper, errors);
        }
        this.functionGenerator = createFunctionGenerator(errors);
    }
}

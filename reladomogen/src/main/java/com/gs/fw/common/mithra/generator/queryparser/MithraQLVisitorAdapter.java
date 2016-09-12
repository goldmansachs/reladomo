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

package com.gs.fw.common.mithra.generator.queryparser;


public class MithraQLVisitorAdapter  implements MithraQLVisitor
{
    public Object visit(SimpleNode node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTCompilationUnit node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTAttributeName node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTRelationalOperator node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTOrExpression node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTAndExpression node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTRelationalExpression node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTIsNullClause node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object visit(ASTequalsEdgePointClause node, Object data)
    {
        return null;
    }

    public Object visit(ASTLiteral node, Object data) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

	public Object visit(ASTInOperator node, Object data)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public Object visit(ASTInLiteral node, Object data)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

}

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

package com.gs.fw.common.mithra.extractor.asm;

import com.gs.mithra.asm.ClassWriter;
import com.gs.mithra.asm.MethodVisitor;
import com.gs.mithra.asm.Opcodes;


public class AsOfAttributeExtractorWriter
{

    public Class createClass(String attributeName, String businessClassName, boolean isInfinityNull)
    {
        String interfaceName = "com/gs/fw/common/mithra/extractor/JustAsOfAttributeExtractor";
        String className = ExtractorWriter.createClassName();
        ClassWriter cw;
        if (isInfinityNull)
        {
            cw = ExtractorWriter.createClassWriterWithConstructor(className, "com/gs/fw/common/mithra/attribute/AsOfAttributeInfiniteNull");
        }
        else
        {
            cw = ExtractorWriter.createClassWriterWithConstructor(className, "com/gs/fw/common/mithra/attribute/AsOfAttribute");
        }
        String attrName = attributeName.substring(0,1).toUpperCase() + attributeName.substring(1);
        addGetMethod(cw, attrName, businessClassName);
        return ExtractorWriter.loadClass(className, cw);
    }

    private void addGetMethod(ClassWriter cw, String attributeName, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "timestampValueOf", "(Ljava/lang/Object;)Ljava/sql/Timestamp;", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, "get"+attributeName, "()Ljava/sql/Timestamp;");
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(1, 2);
        mv.visitEnd();
    }

}

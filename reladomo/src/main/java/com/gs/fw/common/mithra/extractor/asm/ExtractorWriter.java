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

import com.gs.fw.common.mithra.util.TimestampPool;
import com.gs.mithra.asm.ClassWriter;
import com.gs.mithra.asm.Label;
import com.gs.mithra.asm.MethodVisitor;
import com.gs.mithra.asm.Opcodes;

import java.security.SecureClassLoader;
import java.util.concurrent.atomic.AtomicInteger;


public class ExtractorWriter
{

    private static final DirectLoader classLoader = new DirectLoader();

    private static final AtomicInteger count = new AtomicInteger();

    private static final short TIMESTAMP = 10;
    private static final short INT = 20;
    private static final short DOUBLE = 30;
    private static final short OTHER = 40;
    private static final short LONG = 60;
    private static final short BIG_DECIMAL = 70;
    private static final short STRING = 80;
    private static final short TIME = 90;

    private short type;
    private boolean isPrimitive;
    private int loadOpcode;
    private int returnOpcode;
    private int parameterExtraLength;
    private String getterName;
    private String setterName;
    private String getterPrefix;
    private String parameterType;
    private String returnType;

    protected ExtractorWriter(short type, boolean primitive, int loadOpcode, int returnOpcode,
                              int parameterExtraLength, String parameterType, String returnType,
                              String getterName, String setterName, String getterPrefix)
    {
        this.getterName = getterName;
        isPrimitive = primitive;
        this.loadOpcode = loadOpcode;
        this.returnOpcode = returnOpcode;
        this.parameterExtraLength = parameterExtraLength;
        this.parameterType = parameterType;
        this.returnType = returnType;
        this.setterName = setterName;
        this.getterPrefix = getterPrefix;
        this.type = type;
    }

    public static ExtractorWriter intExtractorWriter()
    {
        return new ExtractorWriter(INT, true, Opcodes.ILOAD, Opcodes.IRETURN, 0, "I", "I", "intValueOf", "setIntValue", "get");
    }

    public static ExtractorWriter byteExtractorWriter()
    {
        return new ExtractorWriter(OTHER, true, Opcodes.ILOAD, Opcodes.IRETURN, 0, "B", "B", "byteValueOf", "setByteValue", "get");
    }

    public static ExtractorWriter booleanExtractorWriter()
    {
        return new ExtractorWriter(OTHER, true, Opcodes.ILOAD, Opcodes.IRETURN, 0, "Z", "Z", "booleanValueOf", "setBooleanValue", "is");
    }

    public static ExtractorWriter shortExtractorWriter()
    {
        return new ExtractorWriter(OTHER, true, Opcodes.ILOAD, Opcodes.IRETURN, 0, "S", "S", "shortValueOf", "setShortValue", "get");
    }

    public static ExtractorWriter charExtractorWriter()
    {
        return new ExtractorWriter(OTHER, true, Opcodes.ILOAD, Opcodes.IRETURN, 0, "C", "C", "charValueOf", "setCharValue", "get");
    }

    public static ExtractorWriter floatExtractorWriter()
    {
        return new ExtractorWriter(OTHER, true, Opcodes.FLOAD, Opcodes.FRETURN, 0, "F", "F", "floatValueOf", "setFloatValue", "get");
    }

    public static ExtractorWriter doubleExtractorWriter()
    {
        return new ExtractorWriter(DOUBLE, true, Opcodes.DLOAD, Opcodes.DRETURN, 1, "D", "D", "doubleValueOf", "setDoubleValue", "get");
    }

    public static ExtractorWriter longExtractorWriter()
    {
        return new ExtractorWriter(LONG, true, Opcodes.LLOAD, Opcodes.LRETURN, 1, "J", "J", "longValueOf", "setLongValue", "get");
    }

    public static ExtractorWriter stringExtractorWriter()
    {
        return new ExtractorWriter(STRING, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "Ljava/lang/String;", "Ljava/lang/String;", "stringValueOf", "setStringValue", "get");
    }

    public static ExtractorWriter dateExtractorWriter()
    {
        return new ExtractorWriter(OTHER, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "Ljava/util/Date;", "Ljava/sql/Date;", "dateValueOf", "setDateValue", "get");
    }

    public static ExtractorWriter timeExtractorWriter()
    {
        return new ExtractorWriter(TIME, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "Lcom/gs/fw/common/mithra/util/Time;", "Lcom/gs/fw/common/mithra/util/Time;", "timeValueOf", "setTimeValue", "get");
    }

    public static ExtractorWriter timestampExtractorWriter()
    {
        return new ExtractorWriter(TIMESTAMP, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "Ljava/sql/Timestamp;", "Ljava/sql/Timestamp;", "timestampValueOf", "setTimestampValue", "get");
    }

    public static ExtractorWriter byteArrayExtractorWriter()
    {
        return new ExtractorWriter(OTHER, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "[B", "[B", "byteArrayValueOf", "setByteArrayValue", "get");
    }

    public static ExtractorWriter bigDecimalExtractorWriter()
    {
        return new ExtractorWriter(BIG_DECIMAL, false, Opcodes.ALOAD, Opcodes.ARETURN, 0, "Ljava/math/BigDecimal;", "Ljava/math/BigDecimal;", "bigDecimalValueOf", "setBigDecimalValue", "get");
    }

    protected String getGetterPrefix()
    {
        return getterPrefix;
    }

    protected int getReturnOpcode()
    {
        return returnOpcode;
    }

    protected boolean isTimestamp()
    {
        return type == TIMESTAMP;
    }

    protected boolean isTime()
    {
        return type == TIME;
    }

    protected boolean isString()
    {
        return type == STRING;
    }

    protected boolean isInteger()
    {
        return type == INT;
    }

    protected boolean isLong()
    {
        return type == LONG;
    }

    protected boolean isDouble()
    {
        return type == DOUBLE;
    }

    protected String getReturnType()
    {
        return this.returnType;
    }

    protected int getParameterExtraLength()
    {
        return parameterExtraLength;
    }

    protected boolean isPrimitive()
    {
        return isPrimitive;
    }

    protected boolean isBigDecimal()
    {
        return type == BIG_DECIMAL;
    }

    protected int getLoadOpcode()
    {
        return loadOpcode;
    }

    protected String getGetterName()
    {
        return getterName;
    }

    protected String getSetterName()
    {
        return setterName;
    }

    protected String getParameterType()
    {
        return parameterType;
    }

    public Class createClass(String attributeName, boolean isNullable, boolean hasBusinessDate, String businessClassNameWithDots,
            String businessClassName, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, String superClass, boolean hasSequence, boolean isShadowAttribute)
    {
        String className = createClassName();
        ClassWriter cw = createClassWriterWithConstructor(className, superClass);
        String attrName = attributeName.substring(0,1).toUpperCase() + attributeName.substring(1);
        StringBuilder builder = new StringBuilder(businessClassNameWithDots.length()+4);
        for(int i = 0;i < businessClassNameWithDots.length();i++)
        {
            char c = businessClassNameWithDots.charAt(i);
            if (c == '.')
            {
                builder.append('/');
            }
            else
            {
                builder.append(c);
            }
        }
        builder.append("Data");
        String offHeapDataClassName = null;
        String dataClassNameOrInterface = builder.toString();
        if (offHeapFieldOffset >= 0)
        {
            builder.append("$");
            builder.append(businessClassNameWithDots.substring(businessClassNameWithDots.lastIndexOf('.')+1));
            builder.append("Data");
            offHeapDataClassName = builder.toString()+"OffHeap";
            builder.append("OnHeap");
        }
        String dataClassName = builder.toString();
        if (isShadowAttribute)
        {
            addShadowGetMethod(cw, attrName, dataClassName, offHeapDataClassName);
        }
        else
        {
            addGetMethod(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
        }
        addSetMethod(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
        if (hasBusinessDate)
        {
            addSetValueUntilMethod(cw, attrName, businessClassName);
        }
        if (isPrimitive())
        {
            if (isShadowAttribute)
            {
                addShadowIsNullMethod(cw, attrName, isNullable && isPrimitive, dataClassName, offHeapDataClassName);
            }
            else
            {
                addIsNullMethod(cw, attrName, isNullable && isPrimitive, dataClassName, offHeapDataClassName, businessClassName);
            }
            addSetNullMethod(cw, attrName, isNullable && isPrimitive, dataClassName, offHeapDataClassName, businessClassName);
            addSetValueNullUntilMethod(cw, attrName, isNullable && isPrimitive, hasBusinessDate, businessClassName);
            if (isDouble() && hasBusinessDate)
            {
                addIncrementMethod(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
                addIncrementUntilMethod(cw, attrName, businessClassName);
            }
            if (isInteger())
            {
                addSameVersionMethodInteger(cw, isOptimistic, dataClassNameOrInterface);
            }
            if (isInteger() || isLong())
            {
                addIsSequenceSetMethod(cw, attrName, hasSequence, businessClassName);
            }
        }
        else  if (isBigDecimal() && hasBusinessDate)
        {
            addBigDecimalIncrementMethod(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
            addBigDecimalIncrementUntilMethod(cw, attrName, businessClassName);
        }
        else if (isTimestamp())
        {
            addSameVersionMethodTimestamp(cw, isOptimistic, dataClassNameOrInterface);
            addGetTimestampValueOfAsLong(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
        }
        else if (isTime())
        {
            addGetOffHeapTimeValueOfAsLong(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
        }
        else if (isString() && offHeapDataClassName != null)
        {
            addGetOffHeapValueOf(cw, attrName, dataClassName, offHeapDataClassName, businessClassName);
        }
        cw.visitEnd();
        return loadClass(className, cw);
    }

    private void addGetOffHeapValueOf(ClassWriter cw, String attrName, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        String asIntMethod = "zGet"+attrName+"AsInt";
        String getterMethod = "get"+attrName;
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "offHeapValueOf", "(Ljava/lang/Object;)I", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
        Label l0 = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, l0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, asIntMethod, "()I");
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitLabel(l0);
//        mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
        Label l1 = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, l1);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, asIntMethod, "()I");

        mv.visitInsn(Opcodes.IRETURN);
        mv.visitLabel(l1);
//        mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, getterMethod, "()Ljava/lang/String;");
        mv.visitVarInsn(Opcodes.ASTORE, 2);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "com/gs/fw/common/mithra/util/StringPool", "getInstance", "()Lcom/gs/fw/common/mithra/util/StringPool;");
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/gs/fw/common/mithra/util/StringPool", "getOffHeapAddressWithoutAdding", "(Ljava/lang/String;)I");
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    public static Class loadClass(String className, ClassWriter cw)
    {
        byte[] data = cw.toByteArray();
//        try
//        {
//            FileOutputStream fos = new FileOutputStream("C:/temp/attr/"+className+".class");
//            fos.write(data);
//            fos.close();
//        }
//        catch (IOException e)
//        {
//            e.printStackTrace();
//        }
        return classLoader.load(className.replace('/', '.'), data);
    }

    protected static ClassWriter createClassWriterWithConstructor(String className, String superClass)
    {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

        cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER, className, null,
                superClass, null);
        addConstructor(cw, superClass);
        return cw;
    }

    public static String createClassName()
    {
        return "mithra/gen/Attribute" + count.incrementAndGet();
    }

    private static void addConstructor(ClassWriter cw, String superClass)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superClass, "<init>", "()V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    private void addGetMethod(ClassWriter cw, String attrName, String onHeapDataClassName, String offHeapDataClassName, String businessClassName)
    {
        String getter = getGetterPrefix() + attrName;
        String returnType = getParameterType();
        String getterName = getGetterName();
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, getterName, "(Ljava/lang/Object;)"+returnType, null, null);
        mv.visitCode();
        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, getter, "()"+getReturnType());
            mv.visitInsn(getReturnOpcode());
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, onHeapDataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, onHeapDataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, onHeapDataClassName, getter, "()"+getReturnType());
        mv.visitInsn(getReturnOpcode());
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, getter, "()"+returnType);
        mv.visitInsn(getReturnOpcode());
        mv.visitMaxs(1, 2);
        mv.visitEnd();
    }

    private void addGetTimestampValueOfAsLong(ClassWriter cw, String attrName, String onHeapDataClassName, String offHeapDataClassName, String businessClassName)
    {
        String getter = "zGet" + attrName +"AsLong";
        String businessGetter = "get" + attrName;
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "timestampValueOfAsLong", "(Ljava/lang/Object;)J", null, null);
        mv.visitCode();
        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, getter, "()J");
            mv.visitInsn(Opcodes.LRETURN);
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, onHeapDataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, onHeapDataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, onHeapDataClassName, getter, "()J");
        mv.visitInsn(Opcodes.LRETURN);
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, businessGetter, "()Ljava/sql/Timestamp;");
        mv.visitVarInsn(Opcodes.ASTORE, 2);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        Label ifNotNullLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, ifNotNullLabel);
        mv.visitLdcInsn(new Long(TimestampPool.OFF_HEAP_NULL));
        Label returnLabel = new Label();
        mv.visitJumpInsn(Opcodes.GOTO, returnLabel);
        mv.visitLabel(ifNotNullLabel);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/Timestamp", "getTime", "()J");
        mv.visitLabel(returnLabel);
        mv.visitInsn(Opcodes.LRETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    private void addGetOffHeapTimeValueOfAsLong(ClassWriter cw, String attrName, String onHeapDataClassName, String offHeapDataClassName, String businessClassName)
    {
        String getter = "zGetOffHeap" + attrName +"AsLong";
        String businessGetter = "get" + attrName;
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "offHeapTimeValueOfAsLong", "(Ljava/lang/Object;)J", null, null);
        mv.visitCode();
        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, getter, "()J");
            mv.visitInsn(Opcodes.LRETURN);
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, onHeapDataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, onHeapDataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, onHeapDataClassName, getter, "()J");
        mv.visitInsn(Opcodes.LRETURN);
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, businessGetter, "()Lcom/gs/fw/common/mithra/util/Time;");
        mv.visitVarInsn(Opcodes.ASTORE, 2);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        Label ifNotNullLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, ifNotNullLabel);
        mv.visitLdcInsn(new Long(TimestampPool.OFF_HEAP_NULL));
        Label returnLabel = new Label();
        mv.visitJumpInsn(Opcodes.GOTO, returnLabel);
        mv.visitLabel(ifNotNullLabel);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/gs/fw/common/mithra/util/Time", "getOffHeapTime", "()J");
        mv.visitLabel(returnLabel);
        mv.visitInsn(Opcodes.LRETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    private void addShadowGetMethod(ClassWriter cw, String attrName, String dataClassName, String offHeapDataClassName)
    {
        if (offHeapDataClassName != null)
        {
            throw new RuntimeException("not implemented");
        }
        String getter = "_old"+getGetterPrefix() + attrName;
        String returnType = getParameterType();
        String getterName = getGetterName();
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, getterName, "(Ljava/lang/Object;)"+returnType, null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, getter, "()"+getReturnType());
        mv.visitInsn(getReturnOpcode());
        mv.visitMaxs(1, 2);
        mv.visitEnd();
    }

    private void addSetMethod(ClassWriter cw, String attrName, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        String setter = "set" + attrName;
        String setterName = getSetterName();
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, setterName, "(Ljava/lang/Object;"+ getParameterType()+")V", null, null);
        mv.visitCode();
        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitVarInsn(getLoadOpcode(), 2);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, setter, "("+ getParameterType()+")V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, setter, "("+ getParameterType()+")V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, setter, "("+ getParameterType()+")V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    private void addIsNullMethod(ClassWriter cw, String attrName, boolean isNullablePrimitive, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "isAttributeNull", "(Ljava/lang/Object;)Z", null, null);
        mv.visitCode();
        if (isNullablePrimitive)
        {
            String isNull = "is" + attrName+"Null";
            if (offHeapDataClassName != null)
            {
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
                Label label2 = new Label();
                mv.visitJumpInsn(Opcodes.IFEQ, label2);
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, isNull, "()Z");
                mv.visitInsn(Opcodes.IRETURN);
                mv.visitLabel(label2);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
            Label label = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, isNull, "()Z");
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitLabel(label);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, isNull, "()Z");
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 2);
        }
        else
        {
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 1);
        }
        mv.visitEnd();
    }

    private void addShadowIsNullMethod(ClassWriter cw, String attrName, boolean isNullablePrimitive, String dataClassName, String offHeapDataClassName)
    {
        if (offHeapDataClassName != null)
        {
            throw new RuntimeException("not implemented");
        }
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "isAttributeNull", "(Ljava/lang/Object;)Z", null, null);
        mv.visitCode();
        if (isNullablePrimitive)
        {
            String isNull = "_oldis" + attrName+"Null";
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, isNull, "()Z");
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 2);
        }
        else
        {
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 1);
        }
        mv.visitEnd();
    }

    private void addIsSequenceSetMethod(ClassWriter cw, String attrName, boolean hasSequence, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "isSequenceSet", "(Ljava/lang/Object;)Z", null, null);
        mv.visitCode();
        if (hasSequence)
        {
            String isSet = "zGetIs" + attrName+"Set";
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, isSet, "()Z");
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 2);
        }
        else
        {
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(1, 1);
        }
        mv.visitEnd();
    }

    private void addSetNullMethod(ClassWriter cw, String attrName, boolean isNullablePrimitive, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "setValueNull", "(Ljava/lang/Object;)V", null, null);
        mv.visitCode();
        if (isNullablePrimitive)
        {
            String nullSetter = "set"+attrName+"Null";
            if (offHeapDataClassName != null)
            {
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
                Label l1 = new Label();
                mv.visitJumpInsn(Opcodes.IFEQ, l1);
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, nullSetter, "()V");
                mv.visitInsn(Opcodes.RETURN);
                mv.visitLabel(l1);
            }
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
            Label l0 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, l0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, nullSetter, "()V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(l0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, nullSetter, "()V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(1, 2);
        }
        else
        {
            throwRuntimeException(mv, "not nullable", 1, 2);
        }
        mv.visitEnd();
    }

    private void addSetValueUntilMethod(ClassWriter cw, String attrName, String businessClassName)
    {
        String nullSetter = "setUntil";
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, nullSetter, "(Ljava/lang/Object;"+ getParameterType()+"Ljava/sql/Timestamp;)V", null, null);
        mv.visitCode();
        String setterName = "set"+attrName+"Until";
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitVarInsn(Opcodes.ALOAD, 3+getParameterExtraLength());
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, setterName, "("+ getParameterType()+"Ljava/sql/Timestamp;)V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(3, 4);
        mv.visitEnd();
    }

    private void addSetValueNullUntilMethod(ClassWriter cw, String attrName, boolean isNullablePrimitive, boolean hasBusinessDate, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "setValueNullUntil", "(Ljava/lang/Object;Ljava/sql/Timestamp;)V", null, null);
        mv.visitCode();
        if (isNullablePrimitive && hasBusinessDate)
        {
            String nullSetterName = "set"+attrName+"NullUntil";
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, nullSetterName, "(Ljava/sql/Timestamp;)V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(2, 3);
        }
        else
        {
            throwRuntimeException(mv, "not nullable or dated", 2, 3);
        }
        mv.visitEnd();
    }

    private void addIncrementUntilMethod(ClassWriter cw, String attrName, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "incrementUntil", "(Ljava/lang/Object;DLjava/sql/Timestamp;)V", null, null);
        mv.visitCode();
        String setterName = "increment"+attrName+"Until";
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(Opcodes.DLOAD, 2);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, setterName, "(DLjava/sql/Timestamp;)V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(4, 5);
        mv.visitEnd();
    }

    private void addBigDecimalIncrementUntilMethod(ClassWriter cw, String attrName, String businessClassName)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "incrementUntil", "(Ljava/lang/Object;Ljava/math/BigDecimal;Ljava/sql/Timestamp;)V", null, null);
        mv.visitCode();
        String setterName = "increment"+attrName+"Until";
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.ALOAD, 3);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, setterName, "(Ljava/math/BigDecimal;Ljava/sql/Timestamp;)V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(4, 5);
        mv.visitEnd();
    }

    private void addIncrementMethod(ClassWriter cw, String attrName, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        String getter = getGetterPrefix() + attrName;
        String setter = "set" + attrName;
        String incrementer = "increment" + attrName;
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "increment", "(Ljava/lang/Object;D)V", null, null);
        mv.visitCode();

        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, getter, "()"+getReturnType());
            mv.visitVarInsn(getLoadOpcode(), 2);
            mv.visitInsn(Opcodes.DADD);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, setter, "("+ getParameterType()+")V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, getter, "()"+getReturnType());
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitInsn(Opcodes.DADD);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, setter, "("+ getParameterType()+")V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, incrementer, "("+ getParameterType()+")V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    private void addBigDecimalIncrementMethod(ClassWriter cw, String attrName, String dataClassName, String offHeapDataClassName, String businessClassName)
    {
        String getter = getGetterPrefix() + attrName;
        String setter = "set" + attrName;
        String incrementer = "increment" + attrName;
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "increment", "(Ljava/lang/Object;Ljava/math/BigDecimal;)V", null, null);
        mv.visitCode();

        if (offHeapDataClassName != null)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.INSTANCEOF, offHeapDataClassName);
            Label label2 = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, label2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, offHeapDataClassName);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, getter, "()"+getReturnType());
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/math/BigDecimal", "add", "(Ljava/math/BigDecimal;)"+getReturnType());
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, offHeapDataClassName, setter, "(Ljava/math/BigDecimal;)V");
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(label2);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.INSTANCEOF, dataClassName);
        Label label = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassName);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, getter, "()"+getReturnType());
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/math/BigDecimal", "add", "(Ljava/math/BigDecimal;)"+getReturnType());
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassName, setter, "(Ljava/math/BigDecimal;)V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitLabel(label);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitTypeInsn(Opcodes.CHECKCAST, businessClassName);
        mv.visitVarInsn(getLoadOpcode(), 2);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, businessClassName, incrementer, "("+ getParameterType()+")V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(2, 3);
        mv.visitEnd();
    }

    private void addSameVersionMethodInteger(ClassWriter cw, boolean isOptimistic, String dataClassNameOrInterface)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "hasSameVersion",
                "(Lcom/gs/fw/common/mithra/MithraDataObject;Lcom/gs/fw/common/mithra/MithraDataObject;)Z", null, null);
        mv.visitCode();
        if (isOptimistic)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassNameOrInterface);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassNameOrInterface, "zGetPersistedVersion", "()I");
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassNameOrInterface);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassNameOrInterface, "zGetPersistedVersion", "()I");
            Label l0 = new Label();
            mv.visitJumpInsn(Opcodes.IF_ICMPNE, l0);
            mv.visitInsn(Opcodes.ICONST_1);
            Label l1 = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, l1);
            mv.visitLabel(l0);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitLabel(l1);
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(2, 3);
        }
        else
        {
            throwRuntimeException(mv, "not implemented", 2, 3);
        }
        mv.visitEnd();
    }

    private void addSameVersionMethodTimestamp(ClassWriter cw, boolean isOptimistic, String dataClassNameOrInterface)
    {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "hasSameVersion", "(Lcom/gs/fw/common/mithra/MithraDataObject;Lcom/gs/fw/common/mithra/MithraDataObject;)Z", null, null);
        mv.visitCode();
        if (isOptimistic)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassNameOrInterface);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassNameOrInterface, "zGetPersistedVersion", "()Ljava/sql/Timestamp;");
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitTypeInsn(Opcodes.CHECKCAST, dataClassNameOrInterface);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, dataClassNameOrInterface, "zGetPersistedVersion", "()Ljava/sql/Timestamp;");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/Timestamp", "equals", "(Ljava/sql/Timestamp;)Z");
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitMaxs(2, 3);
        }
        else
        {
            throwRuntimeException(mv, "not implemented", 2, 3);
        }
        mv.visitEnd();
    }

    private void throwRuntimeException(MethodVisitor mv, String message, int maxStack, int maxLocals)
    {
        if (maxStack < 3) maxLocals = 3;
        if (maxLocals < 5) maxLocals = 5;
        mv.visitTypeInsn(Opcodes.NEW, "java/lang/RuntimeException");
        mv.visitInsn(Opcodes.DUP);
        mv.visitLdcInsn(message);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/RuntimeException", "<init>", "(Ljava/lang/String;)V");
        mv.visitInsn(Opcodes.ATHROW);
        mv.visitMaxs(maxStack, maxLocals);
    }

    protected static class DirectLoader extends SecureClassLoader
    {
        protected DirectLoader()
        {
            super(ExtractorWriter.class.getClassLoader());
        }

        protected Class load(String name, byte[] data)
        {
            return super.defineClass(name, data, 0, data.length);
        }
    }

}


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

package com.gs.fw.common.mithra.test.domain;

import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;
import java.util.Date;

public interface DatedAllTypesInterface
{

    void setBooleanValue(boolean b);

    void setByteValue(byte b);

    void setShortValue(short i);

    void setCharValue(char c);

    void setIntValue(int s);

    void setLongValue(long s);

    void setFloatValue(float s);

    void setDoubleValue(double s);

    void setStringValue(String mappingName);

    void setTimestampValue(Timestamp timestamp);

    void setDateValue(Date date);

    void setTimeValue(Time time);

    void setByteArrayValue(byte[] bytes);

    void setNullableByteValue(byte b);

    void setNullableShortValue(short i);

    void setNullableCharValue(char c);

    void setNullableIntValue(int s);

    void setNullableLongValue(long s);

    void setNullableFloatValue(float s);

    void setNullableDoubleValue(double s);

    void setNullableStringValue(String mappingName);

    void setNullableTimestampValue(Timestamp timestamp);

    void setNullableDateValue(Date date);

    void setNullableTimeValue(Time time);

    void setNullableByteArrayValue(byte[] bytes);

    boolean isBooleanValue();

    byte getByteValue();

    short getShortValue();

    char getCharValue();

    int getIntValue();

    long getLongValue();

    float getFloatValue();

    double getDoubleValue();

    String getStringValue();

    Timestamp getTimestampValue();

    Date getDateValue();

    byte[] getByteArrayValue();

    byte getNullableByteValue();

    short getNullableShortValue();

    char getNullableCharValue();

    int getNullableIntValue();

    long getNullableLongValue();

    float getNullableFloatValue();

    double getNullableDoubleValue();

    String getNullableStringValue();

    Timestamp getNullableTimestampValue();

    Date getNullableDateValue();

    byte[] getNullableByteArrayValue();

    boolean isNullableByteValueNull();
    boolean isNullableShortValueNull();
    boolean isNullableCharValueNull();
    boolean isNullableIntValueNull();
    boolean isNullableLongValueNull();
    boolean isNullableFloatValueNull();
    boolean isNullableDoubleValueNull();

    void insert();

    void setId(int i);

    DatedAllTypesInterface getNonPersistentCopy();

    void copyNonPrimaryKeyAttributesFrom(DatedAllTypesInterface allTypes2);

    DatedAllTypesInterface getDetachedCopy();

    void setNullablePrimitiveAttributesToNull();

    DatedAllTypesInterface copyDetachedValuesToOriginalOrInsertIfNew();

    void copyNonPrimaryKeyAttributesUntilFrom(DatedAllTypesInterface allTypes4, Timestamp timestamp);
}

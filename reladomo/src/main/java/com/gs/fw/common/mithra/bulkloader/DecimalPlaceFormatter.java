
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

package com.gs.fw.common.mithra.bulkloader;

import java.math.BigDecimal;

import com.gs.fw.common.mithra.attribute.ToStringFormatter;


public class DecimalPlaceFormatter extends ToStringFormatter
{

    private final int scale;

    public DecimalPlaceFormatter(int scale)
    {
        this.scale = scale;
    }

    public String format(double d)
    {
        BigDecimal decimal = new BigDecimal(d);
        decimal = decimal.setScale(this.scale, BigDecimal.ROUND_HALF_UP);
        return decimal.toString();
    }

    public String format(float f)
    {
        BigDecimal decimal = new BigDecimal(f);
        decimal = decimal.setScale(this.scale, BigDecimal.ROUND_HALF_UP);
        return decimal.toString();
    }

    public String format(BigDecimal bd)
    {
        return bd.toPlainString();
    }
}

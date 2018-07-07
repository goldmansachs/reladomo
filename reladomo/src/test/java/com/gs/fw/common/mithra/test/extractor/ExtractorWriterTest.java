package com.gs.fw.common.mithra.test.extractor;

import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.finder.Attribute;

public class ExtractorWriterTest extends MithraTestAbstract
{
    public void testToString()
    {
        Attribute intAttr = AuditedOrderFinder.orderId ();
        assertTrue (intAttr.getClass ().getName ().endsWith ("_AuditedOrder_orderId"));
        assertEquals ("com/gs/fw/common/mithra/test/domain/AuditedOrder.orderId", intAttr.toString ());

        Attribute asofAttr = AuditedOrderFinder.processingDate ();
        assertTrue (asofAttr.getClass ().getName ().endsWith ("_AuditedOrder_processingDate"));
        assertEquals ("com/gs/fw/common/mithra/test/domain/AuditedOrder.processingDate", asofAttr.toString ());
    }
}

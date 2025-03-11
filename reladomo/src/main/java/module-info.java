module com.gs.reladomo {
    requires java.sql;
    requires java.naming;
    requires java.management;
    
    // For JDK 9+ compatibility
    requires jdk.unsupported;
    
    exports com.gs.fw.common.mithra;
    exports com.gs.fw.common.mithra.attribute;
    exports com.gs.fw.common.mithra.behavior;
    exports com.gs.fw.common.mithra.cache;
    exports com.gs.fw.common.mithra.connectionmanager;
    exports com.gs.fw.common.mithra.database;
    exports com.gs.fw.common.mithra.databasetype;
    exports com.gs.fw.common.mithra.extractor;
    exports com.gs.fw.common.mithra.finder;
    exports com.gs.fw.common.mithra.notification;
    exports com.gs.fw.common.mithra.tempobject;
    exports com.gs.fw.common.mithra.transaction;
    exports com.gs.fw.common.mithra.util;
    
    // Allow reflective access for JDK 9+
    opens com.gs.fw.common.mithra.util to java.base;
}

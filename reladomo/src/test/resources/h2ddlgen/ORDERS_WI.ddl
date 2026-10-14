drop table if exists ORDERS_WI;

create table ORDERS_WI
(
    ORDER_ID int not null,
    ORDER_DATE datetime,
    USER_ID varchar(255),
    DESCRIPTION varchar(50),
    "STATE" varchar(50),
    TRACKING_ID varchar(255),
    FROM_Z datetime not null,
    THRU_Z datetime not null,
    IN_Z datetime not null,
    OUT_Z datetime not null
);


CREATE TABLE flink_test
(FlightDate Date,Year UInt16,name String,city String)
ENGINE = MergeTree(FlightDate, (Year, FlightDate), 8192);

#nc
#   2020-06-05,2001,kebe,beijing
#   2020-06-06,2002,wede,shanghai


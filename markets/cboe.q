/ Fetch vix and put-call ratio data from cboe website

.cboe.prefix:"HTTP/1.0\r\nhost:www.cboe.com\r\n\r\n";
.cboe.url:"http://www.cboe.com";

.cboe.getData:{[sf]
    .eq.fetchUrl .cboe.url,sf
    }

.cboe.getPutCallRatio:{
    result:.cboe.getData "/publish/scheduledtask/mktdata/datahouse/vixpc.csv";
    `vxratio set select from (`date`ratio`puts`calls`total xcol ("DFJJJ";enlist ",") 0:result) where not null date;

    result:.cboe.getData "/publish/scheduledtask/mktdata/datahouse/totalpc.csv";
    `pcratio set select from (`date`calls`puts`total`ratio xcol ("DJJJF";enlist ",") 0:result) where not null date;

    result:.cboe.getData "/publish/scheduledtask/mktdata/datahouse/equitypc.csv";
    `eqratio set select from (`date`calls`puts`total`ratio xcol ("DJJJF";enlist ",") 0:result) where not null date;
    };

.cboe.getVix:{
    result:.cboe.getData "/publish/scheduledtask/mktdata/datahouse/vixcurrent.csv";
    `vix set select from (`date`open`high`low`close xcol ("DFFFF";enlist ",") 0:result) where not null date;
    };

.cboe.fetchData:{
    INFO "Refreshing CBOE put call ratio -tables vxratio, pcratio, eqratio";
    .cboe.getPutCallRatio[];
    INFO "Refreshing CBOE Vix - table vix";
    .cboe.getVix[];
    };

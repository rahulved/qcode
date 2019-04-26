/ Download data from Fed. More information here: https://fred.stlouisfed.org/tags/series
/ Entry point is .fed.fetchData[]
/ To get metadata request an API key at https://research.stlouisfed.org/useraccount/apikey

\l eq.q

.fed.config:("SI";enlist ",") 0:`:fed.csv;
delete from `.fed.config where null series;

.fed.data:();
.fed.metadata:();

.fed.getData:{[a_id;numYears]
    a_id:raze string a_id;
    INFO "Saving ",a_id;
    strDate:{[dt] ssr[string dt;".";"-"]};    
    / id = one of SP500,DJIA,DJTA,NASDAQCOM,RU200PR,RU3000PR,WILL5000IDFC, etc.
    / full list here: https://fred.stlouisfed.org/categories/32255
    result:.eq.fetchUrl "https://fred.stlouisfed.org/graph/fredgraph.csv?cosd=",strDate[`date$-5+.z.d-365.25*numYears],"&coed=",strDate[.z.d],"&range=Custom&mode=fred&id=",a_id;    
    tbl:`date`val xcol ("DF"; enlist ",") 0:result;
    tbl:delete from tbl where null val;
    tbl:update id:`$a_id from tbl;
    if [count .fed.data; delete from `.fed.data where id=`$a_id];
    `.fed.data insert tbl
    };


/ Get API key here - https://research.stlouisfed.org/useraccount/apikey
.fed.apiKey:getenv[`FED_API_KEY];

.fed.getMetadata:{[a_id]
    a_id:raze string a_id;
    data:.eq.fetchUrl "https://api.stlouisfed.org/fred/series?series_id=",a_id,"&api_key=",.fed.apiKey,"&file_type=json";
    data1:.j.k first data;
    if [count .fed.metadata; delete from `.fed.metadata where id=`$a_id];
    `.fed.metadata insert update `$id, "D"$realtime_start, "D"$realtime_end, "D"$observation_start, "D"$observation_end, `$frequency_short,  `$seasonal_adjustment_short from data1`seriess
    };

.fed.fetchData:{
    (.fed.getData .) each flip value exec series, years from .fed.config;
    if [count .fed.apiKey; .fed.getMetadata each exec series from .fed.config];
    };

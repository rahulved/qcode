/ Sync to async gateway using -30!
/ Each connection has a type (say rdb/hdb) and queries are sent to the first available free instance of a type.
/ Only one query is sent at a time to each downstream instance, rest are held in a pending queue on gateway and sent to the next available free instance
/   to ensure optimal use of instances - long running queries do not block other queries while there are free instances available 
/ run as either -
/ q gateway.q -conns gwconns.csv -p 5000
/ or
/ q gateway.q -conns_rdb 5011,6011 -conns_hdb 5012,5013,5014 -p 5000
/ To query: .gw.runQuery[<query dictionary - type!query>; <merge function>]
/ h:hopen 5000
/ h (`.gw.runQuery;`rdb`hdb!2#enlist ({system "p"};`);raze)
/ or
/ h (`.gw.runQueryWithTimeout; `rdb`hdb!2#enlist ({system "sleep 3";system "p"};`);raze; 0D00:00:02)

.gw.opts:.Q.opt[.z.X];

/ Load downstream connection details from csv file
.gw.loadConnsFromCsv:{[csvpath]
    conns:("S*I";enlist ",") 0:csvpath;
    conns:select from conns where not null typ;
    .gw.conns:select typ, url:hsym `$(host,'":",'string[port]) from conns
    };

/ Load downstream connection details from command line
.gw.loadConnsFromCmdLine:{
    connNames:key[.gw.opts] where key[.gw.opts] like "conns_*";
    .gw.conns:raze {([] typ:`$6_string[x]; url:hsym `$"," vs first .gw.opts[x])} each connNames
    };

$[`conns in key[.gw.opts]; 
    .gw.loadConnsFromCsv[hsym `$first .gw.opts[`conns]];
    .gw.loadConnsFromCmdLine[]];

/ Function that can be called on gateway remotely to add a downstream instance on the fly
.gw.addInstance:{[typ]
    `.gw.conns insert (typ;`;1+(exec max id from .gw.conns);.z.w;0Np;0;0;0)
    }

/ Connection timeout from command line    
.gw.connectTimeoutMs:"J"$first .gw.opts`connectTimeoutMs;

.gw.conns:update id:til count i, typ:`g#typ, handle:0Ni, nextConnectionAttempt:.z.p, numConnectAttempts:0, queue:0, totalQueries:0 from  .gw.conns;
.gw.queryId:0;
.gw.queries:([] id:`g#`long$(); input:(); sentTime:`timestamp$(); endTime:`timestamp$(); upstreamHandle:`int$(); mergeFn:(); pending:());
.gw.pendingQueries:()!();
.gw.handleToQueryId:(`int$())!`long$();

/ Connect any downstream instances that haven't been connected yet
.gw.connectAll:{
    toConnect:select from .gw.conns where null handle, nextConnectionAttempt<=.z.p;
    if [not count toConnect;:()];
    toConnect:update handle:@[hopen;;{0Ni}] each (toConnect[`url],\:.gw.connectTimeoutMs) from toConnect;
    toConnect:update numConnectAttempts:?[null handle; numConnectAttempts+1; 0], nextConnectionAttempt:?[null handle; .z.p+`time$numConnectAttempts*.gw.connectTimeoutMs; 0Np] from toConnect;
    .gw.conns:.gw.conns lj `id xkey toConnect;
    {.gw.pendingQueries[x]:()} each exec distinct typ from .gw.conns;    
    };

/ Initial connection attempt
.gw.connectAll[];

/ On disconnect - if downstream instance disconnected, then attempt reconnect if url is known and error our any queries running on this handle
/ If upstream instance (ie query caller) disconnected then remove queries for that caller from pending queue
.z.pc:{[h]
    // Downstream connection may have gone away
    update handle:0Ni, nextConnectionAttempt:.z.p, numConnectAttempts:0, queue:0 from `.gw.conns where handle=h, not null url;
    // If its an instance that was added on the fly (ie don't know its url) we aren't going to try and reconnect
    delete from `.gw.conns where handle=h, null url;
    / now cancel any queries that were waiting on this handle. There can only be one query waiting on the handle
    if [not null .gw.handleToQueryId[h];
        upstreamHandle:exec first upstreamHandle from .gw.queries where id=.gw.handleToQueryId[h];
        if [not null upstreamHandle; 
            @[-30!;(upstreamHandle;1b;"closed");{0N!e}];
            .gw.deletePendingQueriesForUpstreamHandle[upstreamHandle]
        ];
        .gw.handleToQueryId[h]:0Nj
    ];    
    // Client may have gone away         
    .gw.deletePendingQueriesForUpstreamHandle[h];
    delete from `.gw.queries where upstreamHandle=h
    };

/ If client disconnected then remove any of its pending queries from queue
.gw.deletePendingQueriesForUpstreamHandle:{[h]
    qids:exec distinct id from .gw.queries where upstreamHandle=h;
    if [count qids;
        .gw.pendingQueries:key[.gw.pendingQueries]!value[.gw.pendingQueries]@'(where each value not .gw.pendingQueries[;;0] in qids)
    ]
    };

/ If any connections are available and there is a pending query for that connection type, then send a query to that connection from the queue
.gw.sendNextQuery:{[atyp]
    / No queries for this downstream type
    if [not count .gw.pendingQueries[atyp]; :()];
    conns:select from .gw.conns where typ=atyp, not null handle, queue=0;
    if [not count conns; :()];
    conn:first conns;
    idQry:first .gw.pendingQueries[atyp];
    update queue:queue+1, totalQueries:totalQueries+1 from `.gw.conns where handle=conn`handle;        
    neg[conn`handle] (.gw.remoteCall;last idQry;`.gw.callback;first idQry);
    .gw.pendingQueries[atyp]:1 _ .gw.pendingQueries[atyp]
    };

/ Externally callable function
.gw.runQuery:{[queryDict; mergeFn]
    .gw.runQueryWithTimeout[queryDict; mergeFn; 0Nn]
    };

/ Externally callable function with timeout. Timeout specified as timespan
.gw.runQueryWithTimeout:{[queryDict; mergeFn; timeout]
    rh:.z.w;
    if [not all key[queryDict] in et:exec distinct typ from .gw.conns; '"noconns_","_" sv string key[queryDict] except et];
    queryId:.gw.queryId;
    {[qid;qd;t] .gw.pendingQueries[t],:enlist (qid; qd[t])}[queryId; queryDict] each key queryDict;        
    `.gw.queries insert (queryId; enlist queryDict; .z.p; .z.p+timeout; rh; mergeFn; ());        
    .gw.queryId:.gw.queryId+1;
    / Try to send queries to downstream instances that don't have a queue
    .gw.sendNextQuery each key queryDict;
    -30!(::)
    };

/ Function to call on downstream instance
.gw.remoteCall:{[x; callback; qid]   
    neg[.z.w] (callback;@[{(0b;value x)};x;{[e] (1b; e)}]; qid)
    };

/ Callback function to process result from downstream system
.gw.callback:{[result; qid]
    update queue:queue-1 from `.gw.conns where handle=.z.w;                
    if [not count select from .gw.queries where id=qid; :()];    //client may have disconnected or timed out
    update pending:(pending,'enlist enlist result) from `.gw.queries where id=qid;
    qry:first select from .gw.queries where id=qid;        
    if [count[key flip qry`input]=count[qry`pending];
        isErr:sum[qry[`pending][;0]]>0;
        res:$[isErr; "," sv (qry[`pending][;1]) where (qry[`pending][;0]); (qry`mergeFn)@qry[`pending][;1]];        
        @[-30!;(qry`upstreamHandle;isErr;res);{[e] 0N!e}];        
        delete from `.gw.queries where id=qid
    ];
    .gw.sendNextQuery[exec first typ from .gw.conns where handle=.z.w]
    };

/ Deal with reconnections and query timeouts
.z.ts:{    
    .gw.connectAll[];
    expiredQueries:select from .gw.queries where not null endTime, endTime<.z.p;
    if [count expiredQueries;
        {[h] -30!(h;1b;"timeout")} each expiredQueries`upstreamHandle;    
        .gw.pendingQueries:key[.gw.pendingQueries]!value[.gw.pendingQueries]@'(where each value not .gw.pendingQueries[;;0] in expiredQueries`id);
        delete from `.gw.queries where id in expiredQueries`id
    ];
    };

/ Granularity for query timeouts is 1s - change this if required
system "t 1000";


\
.gw.conns
.gw.queries
.gw.pendingQueries
\
.gw.runQueryWithTimeout[enlist[`rdb]!enlist ({system "sleep 5"};`); raze; 0D00:00:03]
.gw.runQuery[`rdb`hdb!2#enlist ({system "p"};`); raze]
h:hopen `::5000
res:();
do[5;res,:h (`.gw.runQuery;`rdb`hdb!2#enlist ({system "p"};`); raze)]
.gw.runQueryWithTimeout[enlist[`rdb]!enlist ({system "sleep 5};`); raze; 0D00:00:03]
.gw.queries
workerHandles:hopen each 5001 5002;

maxtime:0D00:00:04;

pending:()!();
sendtime:()!();

remoteCall:{[x; callback; rh]   
    neg[.z.w] (callback;@[{(0b;value x)};x;{[e] (1b; e)}]; rh)
    };

callback:{[result; rh]
    if [not rh in key sendtime; :()];
    pending[rh]:pending[rh],enlist result;
    0N!"received result for ",string[rh]," Count = ",string[count[pending[rh]]];    
    
    if [count[workerHandles]=count pending[rh];
        isErr:sum[pending[rh;;0]]>0;
        res:$[isErr; "," sv (pending[rh;;1]) where (pending[rh;;0]); raze pending[rh;;1]];
        -30!(rh;isErr;res);
        `pending set (enlist rh) _ pending;
        `sendtime set (enlist rh) _ pending;
    ]
    };

.z.ts:{    
    -30!/:(where maxtime<.z.p-sendtime),\:(1b;"timeout");
    `pending set (where maxtime<.z.p-sendtime) _ pending;
    `sendtime set (where maxtime<.z.p-sendtime) _ sendtime;    
    };

system "t 1000";

.z.pg:{[x]
    0N!.Q.s[x];
    pending[.z.w]:();
    sendtime[.z.w]:.z.p;
    neg[workerHandles]@\:(remoteCall;x;`callback;.z.w);    
    -30!(::)
    };

workerHandles@\:(set;`myfunc;{0N!"Got call"; system "sleep ",string[first 1?10]; r:first 1?100; $[r<50; '"error_",string[r]; r]});

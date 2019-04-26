init:{
	csvpath:`:gwconns.csv;
	`conns set ("S*I";enlist ",") 0:csvpath;
	delete from `conns where null typ;
	system each "q -p ",/:string[conns`port];
	system "q gateway.q -p 5000 -conns gwconns.csv"
	}

.test.test1:{
	h:hopen 5000;
	res:h (`.gw.runQuery; `rdb`hdb!2#enlist ({system "p"};`); raze);
	hclose h;
	0N!.Q.s[res];
	2=count distinct res
	};

.test.test2:{
	h:hopen 5000;
	res:();
	do[5000; res,:enlist h (`.gw.runQuery; `rdb`hdb!2#enlist ({system "p"};`); raze)];
	0N!.Q.s[count each group asc each res];
	3000=count res
	};

.test.test3:{
	ports:7001 7002;
	fn:{
		h:hopen 5000;
		res:();
		do[50000; res,:enlist h (`.gw.runQuery; `rdb`hdb!2#enlist ({system "p"};`); raze)];
		hclose h;
		neg[.z.w] enlist[system "p"]!enlist[res]
	};
	system each "q -p ",/:string[ports];
	
	`res set ()!();
	`passfail set ();	
	.z.ps:{
		0N!"Received response";
		`res set res,x;
		p:first key x;
		r:first value x;
		0N!"Port ",string[p];
		c:count each group asc each r;
		0N!.Q.s	c;
		0N!sum[c];
		`passfail set passfail,sum[c]=50000;
		.z.w@"\\";
		if [2=count passfail;
			system "x .z.ps";
			0N!$[all passfail; "Passed test3"; "Failed test3"];
			delete res, passfail from `.
		];
	};
	system "sleep 5";
	h:hopen each ports;
	neg[h]@\:(fn;`);
	};
	
init[];

runAll:{
	fns:system "f .test";
	rets:{	
		0N!"Running ",string[x];
		ret:@[value ` sv (`.test;x);`;{[e] 0N!e; 0b}];
		0N!"Done running ",string[x]," - "("Failed";"Passed")@ret;
		ret
	} each fns;
	$ [all rets; "Passed"; "Failed"]
	};

	
			
	
	
	


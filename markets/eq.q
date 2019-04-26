\l log4q.q

convertEpoch:{"p"$1970.01.01D+1000000j*x};

/ Pivot functions from https://code.kx.com/wiki/Pivot

piv:{[t;k;p;v;f;g]
 v:(),v;
 G:group flip k!(t:.Q.v t)k;
 F:group flip p!t p;
 count[k]!g[k;P;C]xcols 0!key[G]!flip(C:f[v]P:flip value flip key F)!raze
  {[i;j;k;x;y]
   a:count[x]#x 0N;
   a[y]:x y;
   b:count[x]#0b;
   b[y]:1b;
   c:a i;
   c[k]:first'[a[j]@'where'[b j]];
   c}[I[;0];I J;J:where 1<>count'[I:value G]]/:\:[t v;value F]};

pivot:piv[;;;;
            {[v;P]`$raze each string raze P[;0],'/:v,/:\:P[;1]};
            {[k;P;c]k,(raze/)flip flip each 5 cut'10 cut raze reverse 10 cut asc c}];

.eq.fetchUrl:{[fullUrl]
    INFO "Fetching ",fullUrl;
    system "curl -s -L '",fullUrl,"' 2>&1"
    };


.eq.getNasdaq100Names:{
    result:.eq.fetchUrl "http://www.nasdaq.com/quotes/nasdaq-100-stocks.aspx?render=download";    
	update date:.z.d from `sym`name`lastPrice`netChange`pctChange`shareVolume`nasdaq100Points xcol ("S*FFFJF";enlist ",") 0:result
    };

.eq.getSP500Names:{
    result1:.eq.fetchUrl "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv";
    update date:.z.d from `sym`name`sector xcol ("S*S";enlist ",") 0:result1
    };


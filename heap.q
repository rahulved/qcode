/Heap (priority queue) implementation in q
/Always returns the 
/To create a new heap:
/p:newheap[];  <-- returns a pointer to the newly created heap
/put[p;<val>]; <-- add a value to the heap
/take[p]; <-- get the lowest value from the heap
/todo: make highest or lowest value configurable 

heaps:() 
compfns:()
           
newheap:{
    heaps,:enlist ();
    -[count heaps;1]} 

put:{[ptr;val]
    heaps[ptr],:val;
    heaps[ptr]:bubble_up[heaps[ptr]]}
    
take:{[ptr]
    if [0 = count heaps[ptr]; :()];
    v:heaps[ptr][0];
    heaps[ptr]:bubble_down[heaps[ptr]];
    v}
	
bubble_up:{bubble_up_impl[x;-[count x;1]]}    

bubble_up_impl:{[x;pos]
    if [pos<=0; :x];
    parent:floor -[pos;1]%2;
    if [x[pos]<x[parent];
        t:x[parent];
        x[parent]:x[pos];
        x[pos]:t;
    ];
    bubble_up_impl[x;parent]}
	
bubble_down:{
    if [1 >= count x; :x];        
    x[0]:last x;
    x:x _ -[count x;1];  
    bubble_down_impl[x;0]}
	
bubble_down_impl:{[x;pos]
    child1:1+pos*2;
    child2:2+pos*2;    
    if [child1>=count x; :x];
    postoswap:child1;
    if [child2<count x; if [x[child2]<x[child1]; postoswap:child2;]];
    if [x[pos]>x[postoswap];
        t:x[pos];
        x[pos]:x[postoswap];
        x[postoswap]:t;
    ];
    bubble_down_impl[x;postoswap]}
	
	
test1:{
    p:newheap[];
    lst:30000?til 10000000;
    c:0;
    do[count lst;put[p;lst[c]];c:c+1];
    lst_s:asc lst;
    do [count lst;
        v1:lst_s[0];
        v2:take[p];
        if [v1<>v2; show (v1;v2;count v1);'notequal];
        lst_s:lst_s _ 0;
        ];
   `ok}

metric_add:{
    p:newheap[];
    lst:30000?til 10000000;
    c:0;
    do[count lst;put[p;lst[c]];c:c+1]}

	
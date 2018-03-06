for i in 2 3 4 5 6 7 8 9 10 11 12 13; do echo INPUT$i has Data= `find /home/user/INPUT$i/Data -type f -name '*.avro'  
| wc -l` and schema=`find /home/user/INPUT$i/Schema -type f -name '*.avsc'  
| wc -l`and size=`du -h --max-depth=1 /home/user/INPUT$i/Data`; done;



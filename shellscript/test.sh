for i in 2 3 4 5 6 7 8 9 10 11 12 13; do echo INPUT$i has Data= `find /home/user/INPUT$i/Data -type f -name '*.avro'  
| wc -l` and schema=`find /home/user/INPUT$i/Schema -type f -name '*.avsc'  
| wc -l`and size=`du -h --max-depth=1 /home/user/INPUT$i/Data`; done;

/*
INPUT2 has Data= 145776 and schema=145776and size=89G /home/user/INPUT2/Data
INPUT3 has Data= 163444 and schema=163444and size=99G /home/user/INPUT3/Data
INPUT4 has Data= 182683 and schema=182683and size=107G /home/user/INPUT4/Data
INPUT5 has Data= 201841 and schema=201841and size=114G /home/user/INPUT5/Data
INPUT6 has Data= 208136 and schema=208136and size=114G /home/user/INPUT6/Data
INPUT7 has Data= 218652 and schema=218652and size=119G /home/user/INPUT7/Data
INPUT8 has Data= 184978 and schema=184978and size=107G /home/user/INPUT8/Data
INPUT9 has Data= 192916 and schema=192916and size=117G /home/user/INPUT9/Data
INPUT10 has Data= 124346 and schema=124346and size=70G /home/user/INPUT10/Data
INPUT11 has Data= 175862 and schema=175862and size=99G /home/user/INPUT11/Data
INPUT12 has Data= 171585 and schema=171585and size=93G /home/user/INPUT12/Data
INPUT13 has Data= 221539 and schema=221539and size=126G /home/user/INPUT13/Data 
*/



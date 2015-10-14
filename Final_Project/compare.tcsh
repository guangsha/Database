#!bin/tcsh

rm ./output/*/*

set i = 0
while ( $i < = 9 )

echo $i


./main.o testcases/test0$i 

diff output/dbs/db0$i.db correct/dbs/db0$i.db
diff output/log/log0$i.log correct/logs/log0$i.log

@ i++
end

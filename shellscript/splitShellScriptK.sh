#!/bin/bash
#=====================================================================
# syntax : ./splitShellScript.sh <executionstring> <folderpath> 
# example: ./splitShellScript.sh 20170330 /home/kks/splittry/finaltest 
# This shell script will split the data of size > few GB to MB 
# and maintains the header and detail section intact
# Contributors : Kirthi, Amit
# Date : Apr 14 '17
#=====================================================================
start=`date +%s`
excStr=$1
#a="/home/xxhdxx/SDIX/LongForm/Long_Format_X_0"
a=$2
homedir=$a/$excStr # Define home directory in a variable
if [ -n "$a"];
then
	a="/home/cne2hdfs/SDIN/LongFormat/Long_Format_3_0"
fi
echo "****************************************************************"
echo "Started to work on ---- >  "$a
echo "Execution String : -----> "$excStr
echo "****************************************************************"

echo "STEP 1 : WARNING ***** Looking for done flag in all folders. If the scripts stops here then it has some directory which does not have done flag ***"

for i in `ls $homedir`
do 
	for j in `ls $homedir/$i`
	do 
		echo $i : $j  
		if [ -f $homedir/$i/$j/done_flag.txt ]
		then doneflag=0
		else 
			echo " File  - > $homedir/$i/$j/done_flag.txt not found! Exit here!!!!"
			exit 1
		fi
	done
done;
#for i in `ls $a/$excStr`;do for j in `ls $a/$excStr/$i`; do echo $i : $j : `if [ -f $a/$excStr/$i/$j/done_flag.txt ];then  echo " exists"; else echo " no"`;done;done;

echo " STEP 2 : Creating relevant directory"
for i in `ls $homedir`;do for j in `ls $homedir/$i`; do echo $i : $j :`mkdir $homedir/$j/split`;`mkdir $homedir/$j/final`;`mkdir $homedir/$j/original`;`mkdir $homedir/$j/log`;`mkdir $homedir/$j/header`;done;done; >> /tmp/SP_mkdirlog.log;

exit 1
echo "Finding the average size of the folder " 
avg=find $homedir/Bit3_XXX/* -ls | awk '{sum += $7; n++;} END {print sum/1024^2/n;}'
if [ avg < 256];
then 
	echo " Avergae file size is < 256 hence exit here"
	exit 1	

echo "Seperating the header and detail file"
for i in `ls $a/$excStr/Bit3_XXX/*/*.csv`; do for j in `ls $i | sed 's|\(.*\)/.*|\1|'`; do echo $i : $j : $(basename ${i} .${i##*.}):`awk '/#Header Start/{flag=1;next}/#Header End/{flag=0}flag' $i  > $j/header/$(basename ${i} .${i##*.})Header.csv`;echo $i :`awk '/#Detail Start/{flag=1;next}/#Detail End/{flag=0}flag' $i  > $j/split/$(basename ${i} .${i##*.})Detail.csv`;done;done; >> /tmp/SP_seperatedetailheaderlog.log


echo " spliting all the files now"
for i in `ls $a/$excStr/Bit3_XXX/*/*.csv`; do for j in `ls $i | sed 's|\(.*\)/.*|\1|'`; do echo $i : $j :`/usr/anaconda/bin/python /home/kks/source/COMMON/python/splitCSV_jw.py $j/split 1024 256 $j/original/ 1 /tmp/SP_splitDetailLog.log`; done; done;

# Cannot be moved the next step will fail. with another approach moving to the last step
#echo "moving the orginal source file to $a"
#echo "******* Please note both a and x at this time is a global variable; we need not redefine it******"
#for i in `ls $a/$excStr/Bit3_XXX/*/*.csv`; do for j in `ls $i | sed 's|\(.*\)/.*|\1|'`; do echo $i : $x;mv $i $a"/Original/"$x;done;done; >> /tmp/SP_MoveSourceFileTpOrginalLog.log

echo " %%%% Finally Merge the split file with header %%%%%"
count=1;for m in `ls $a/$excStr/Bit3_XXX/*/*.csv |sed 's|\(.*\)/.*|\1|' |uniq`;do for i in `ls $a/$excStr/Bit3_XXX/*_temp*/split/*.csv`;do n=`ls $i | sed -e 's/\.[^.]*$//' |sed -r 's/^.+\///'`;b=`echo $n|sed -e "s|\(.*\)Detail.*|\1|"`;echo $b;x=$m"/header/"$b"Header.csv";y=$m"/"$n"_"$count".csv";echo $x : $y ;`echo "#Header Start" > $y`;`echo "$(<$x)" >> $y`;`echo "#Header End" >> $y`;`echo "#Detail Start" >> $y`;`cat $i >> $y`;`echo "#Detail End" >> $y`;count=`expr $count + 1`;done;done;

echo " Move the files to its respective original directory for backup"
for i in `ls $a/$excStr/Bit3_XXX/*/*.csv`; do for j in `ls $i | sed 's|\(.*\)/.*|\1|'`; do echo $i : $j;`find $j -maxdepth 1 -size +500M -print -type f -exec mv "{}" $x \;`;done;done;

#echo " Finally remove all thesubfolders created during the process" 
#rm -r final log original split header
end=`date +%s`
SECONDS=$((end-start))
ELAPSED="Elapsed: $(($SECONDS / 3600))hrs $((($SECONDS / 60) % 60))min $(($SECONDS % 60))sec"

echo " ------   $ELAPSED    -----------"
echo "****************************************************************"
echo "==================End================="
echo "****************************************************************"

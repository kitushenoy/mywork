REM remove header from all the files in the directory 
for n in *.csv ; do awk '{if(NR>1)print}' $n >> ../sample_withoutheader/$n; done;

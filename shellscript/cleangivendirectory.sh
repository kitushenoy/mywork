extract the dates from db
select exec_string from xy.xx_Data_Ingestion_Status
	where datasource='xx' and exec_string >= '20190230' and exec_string < '20190730' and status like '%xxcomplete'#logs=(20190301_00 20190301_01) 
	lpath="/home/xyz/DI/xx/Log/xx_" 
	echo "TESTING: List out the folders to delete" 
	for log in ${logs[*]};do if [ -n "$lpath$log" ]; then echo "$lpath$log exist and ready to delete"; ls $lpath$log; ls $lpath$log;else echo "$lpath$log does not exist"; fi;done; 

		echo "PROD: DELETING" 
		read -n 1 -p "Input Selection:" mainmenuinput 
		if [ "$mainmenuinput" = "Y" ]; then 
			for log in ${logs[*]};do if [ -n "$lpath$log" ]; then echo "$lpath$log exist and ready to delete"; rm -r $lpath$log; else echo "$lpath$log does not exist"; fi;done; 

		elif [ "$mainmenuinput" = "N" ];then 
			quitprogram 
		else 
			echo "You have entered an invallid selection!" 
			echo "Please try again!" 
			echo "" 
			echo "Press any key to continue..." 
		        clear 
		        mainmenu 
		fi 
} 
#This builds the main menu and routs the user to the function selected. 
	 
mainmenu 
	 
# This executes the main menu function. 
# Let the fun begin!!!! WOOT WOOT!!!! 
read -n 1

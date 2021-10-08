/*

This do file calculates travel distance between all pairs of french cantons

*/

/* 

Running osrmtime command requires downloading files and pasting them in directory 
manually. No ssc install.
Link I used for explanations: https://github.com/christophrust/osrmtime
I have uploaded the folder of files on dropbox. Paste the folder wherever you want
Then run command below to install the files, making sure to add the location of the files in the from(" "):

net describe osrmtime, from("/path/to/extracted/files")

Then run:
mkdir mymaps

You should then download this map:
https://download.geofabrik.de/europe/france-latest.osm.pbf

Add the map in C:\osrm\mymaps

Then run
cd "C:\osrm"
osrmprepare , mapfile("mymaps/france-latest.osm.pbf") profile(car) osrmdir("C:\osrm")

This last step prepares the map to run osrmtime more efficiently, it takes quite some time to run (1h) but you only need to do it once

You are then good to go

The file should run smoothly until osrmtime command, which should take a few hours

*/

*Change below to your own directory
cd "C:\Users\cypri\OneDrive\RA app\French firms RA\Distance task"


******* Prepare datasets *******


*Keep only cities that are in both coverage and DADS. Dataset used is the final one obtained from my merging task do-file
use "DADS_coverage", clear
keep depcom
duplicates drop
save "DADS_coverage_depcom", replace

*Import 2007 canton codes. From https://www.insee.fr/fr/information/2560646
import delimited "comsimp2007.txt", clear delimiters("") 
save "commune2007", replace
destring dep, force replace
gen can = ct + (100)*dep
gen depcom = 1000*dep + com
drop if depcom == .
drop if depcom >= 96000 | depcom < 1001
keep dep ncc can depcom
order depcom ncc can dep
save "commune2007mod", replace

*We have no coverage/DADS data for 288 cities. 45 using mismatch because of Paris, Lyon and Marseille
use "commune2007mod", clear
merge 1:1 depcom using "DADS_coverage_depcom"
keep if _merge==3
drop _merge
save "DADS_coverage_depcom_name_can", replace

*Lyon and Paris do not have canton so I created made-up codes. Used same method for Marseille
*Actual canton numbers do not matter
*Prepare commune 2021 file to obtain Lyon, Paris, Marseille info
import delimited "commune2021", clear delimiters(";") 
save "commune2021", replace
rename com depcom
rename ncc city
*For some reason they kept the old name for some cities so that there are two names for a same depcom. I drop the old name
duplicates tag depcom, gen(tag)
drop if tag==1 & reg==.
replace dep = floor(depcom/1000)
keep depcom dep city
duplicates drop
drop if depcom == .
drop if depcom >= 96000 | depcom < 1001
save "commune2021mod", replace

*Assign canton number
use "commune2007mod", clear
merge 1:1 depcom using "DADS_coverage_depcom"
keep if _merge==2
drop _merge
keep depcom
merge 1:1 depcom using "commune2021mod"
keep if _merge==3
drop _merge
tostring depcom, replace
gen first_digit = substr(depcom, 5, 1)
destring depcom, replace
destring first_digit, replace
gen can = dep*100 + first_digit
drop first_digit
append using "DADS_coverage_depcom_name_can"
replace city = ncc if city == ""
drop ncc
save "DADS_coverage_depcom_name_can2", replace

******* Calcule distances *******


*Contrary to georoute, osrmtime requires geocoordinates to run

*I randomly select one city per canton to obtain the geocoordinates
use "DADS_coverage_depcom_name_can2", clear
sort can city
by can: gen serial_number_can=_n
keep if serial_number_can==1
drop serial_number_can
gen cntry = "FRANCE"
order depcom city can dep cntry
save "DADS_coverage_depcom_name_can2", replace

*Same problem as before some cities have an homonym, which leads to the wrong geocoordinates
*It would be too time-consuming to check if the 3654 cities have the right geocoordinates
*But below I correct any mistake I catch
use "DADS_coverage_depcom_name_can2", clear
replace city = "LA GRAVE" if city == "GRAVE"
save "DADS_coverage_depcom_name_can3", replace


*Run georoute to obtain the geocoordinates of chosen city. Should take a few minutes
use "DADS_coverage_depcom_name_can3", clear
georoute, herekey(TVGmAWwUKDaklcv60h1voSSgr97PUqa3CZaknQlaXdw) startad(city dep cntry) endad(city dep cntry) km distance(dist) coordinates(p1 p2)
keep depcom city can dep cntry p1_x p1_y
save "geocord", replace

*Sometimes the name of the city is correct but I still get wrong geocoordinates
*Here I correct those that I find that are not correct
use "geocord", clear
replace p1_x = 49.242204 if can == 1415
replace p1_y = -0.030729 if can == 1415
replace p1_x = 48.073493 if can == 7208
replace p1_y = -0.059757 if can == 7208

*replace p1_x == 43.257932 & p1_y == 1.115045 if can==3127
save "geocord", replace


*Prepare geocord2 file to add necessary information for destination cantons after fillin command
use "geocord", clear
rename can can2
rename p1_x p2_x
rename p1_y p2_y
drop depcom dep city cntry
save "geocord2", replace

use "geocord", clear
drop depcom dep city cntry
gen can2=can
compress
*Create each pair of canton, should take a minute
fillin can can2
sort can _fillin
gen serial_number= _n
*Assign correct information to each starting canton
foreach v of var serial_number {
	replace can=can[_n-1] if can==.
}
foreach v of var serial_number {
	replace p1_x=p1_x[_n-1] if p1_x==.
}
foreach v of var serial_number {
	replace p1_y=p1_y[_n-1] if p1_y==.
}
drop _fillin serial_number
merge m:1 can2 using "geocord2"
keep if _merge==3
drop _merge
sort can can2
*We only need each distance once so to reduce requests to osrmtime we can add code belox
keep if can2>=can & can2!=can
save "filledin_geocoord", replace

*We have missing distances because for some reasons we do not have the geocoordinates
*of can 3497 when it is the destination. I correct this here
use "filledin_geocoord", clear
replace p2_x = 43.54877 if can2 == 3497 & p2_x == .
replace p2_y = 3.70793 if can2 == 3497 & p2_y == .
save "filledin_geocoord", replace

*This should take between 8 hours and 2 days depending on system power and internet connection
use "filledin_geocoord", clear
cd "C:\osrm"
osrmtime p1_x p1_y p2_x p2_y , mapfile("mymaps/france-latest.osrm") osrmdir("C:\osrm")
save "test2", replace



/*
Additional comments

I choose randomly a city per canton. Ideally we would choose the most central city 
per canton. But it would be too time-intensive, and probably not so useful.

Canton size is inversely proportional to population density. I assume that firms 
are more concentrated in more densely populated areas, where canton size is smaller 
and thus where our approximation is better. 
Our approximation will be poorest for rural firms outsourcing to rural firms. 
Perhaps we can make robustness checks excluding cantons that have a number of cities 
over some threshold (rural cantons).

*/

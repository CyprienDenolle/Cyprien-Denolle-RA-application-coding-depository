/*

This do file combines the datasets “DADS_Est_depcom_codes.dta” and “coverage_broadband_all_ml.dta”,
and attempts to improve the merge by checking for miscodes.

*/

*Change below to your own directory
cd "C:\Users\cypri\OneDrive\RA app\French firms RA\Merging task"


******* Prepare change in boundaries dataset *******

*File with history of change in boundaries from https://www.insee.fr/fr/information/5057840
import delimited "mvtcommune2021", clear delimiters(";") 

generate date_n = date(date_eff, "DMY", 2050)
format date_n %td
rename date_n date
drop date_eff
order mod date
save "mvtcommune2021all", replace


******* Clean DADS *******

use "DADS_Est_depcom_codes", clear

*Depcoms with letters are non-metropolitan departments, and thus should be dropped as in “coverage_broadband_all_ml.dta”
destring(depcom), replace force

*Years without depcom
drop if missing(depcom)

*Drop non-metropolitan France (like in “coverage_broadband_all_ml.dta”) and depcom 999 seems to be a miscode
drop if depcom >= 96000 | depcom < 1001

save "DADS_Est_depcom_codes_mod", replace


******* Investigate coverage mismatch (using) *******

*Initial: 40145
*Have no DADS data on 1995
use "DADS_Est_depcom_codes_mod", clear
merge 1:1 depcom year using "coverage_broadband_all_ml"
keep if _merge==2
drop if year==1995
*Now only 4119

use "DADS_Est_depcom_codes_mod", clear
merge m:m depcom  using "coverage_broadband_all_ml"
keep if _merge==2
drop if year==1995
keep depcom
duplicates drop
save "No DADS but coverage for all years", replace
/*
For these 15 depcom we have coverage data but no DADS for all years. Likely to be
small cities with internet but no firm
Examples (pop obtained from googling):
5094 (population = 14)
9090 (population = 38)
9156 (population = 21)
26245 (population = 20)
31046 (population = 13)
31369 (population = 40)
31465 (population = 14)
31559 (population = 4)
*/

use "DADS_Est_depcom_codes_mod", clear
merge 1:1 depcom year using "coverage_broadband_all_ml"
keep if _merge==2
drop if year==1995
keep depcom
duplicates drop
save "No DADS but coverage for some or all years", replace
*Also have 1576 cities with firms only in some years. Also probably because they are small cities

*These should explain the 4119 mismatched observations not matched from coverage (using)


******* Investigate DADS mismatch (master) *******

*Find list of cities in DADS that mismatch
use "DADS_Est_depcom_codes_mod", clear
merge 1:1 depcom year using "coverage_broadband_all_ml"
keep if _merge==1
sort depcom year
keep depcom
duplicates drop
levelsof depcom, local(levels) 
save "list DADS mismatch", replace
*323 cities to check: these cities are in DADS but not in coverage

use "mvtcommune2021all", clear
gen tag=.
foreach l of local levels {
	replace tag = 1 if com_av == `l'
}
keep if tag==1
drop tag
order com_av com_ap date
sort com_av date
rename com_av depcom
rename com_ap depcom2
save "list DADS mismatch in mvtcommune2021all", replace
*These cities mismatched because of miscoding?


******* Creating dataset with perfect match of DADS and coverage (matchindic == 1) *******

use "DADS_Est_depcom_codes_mod", clear
merge 1:1 depcom year using "coverage_broadband_all_ml"
keep if _merge == 3
drop  _merge
gen matchindic = 1
save "DADS_coverage_perfect_match", replace


******* Creating dataset with artifical merge of DADS and coverage (matchindic == 0) *******

*Preparing change in boundaries file for the merge
use "list DADS mismatch in mvtcommune2021all", clear
keep depcom
duplicates drop
save "list DADS mismatch in mvtcommune2021depcom", replace

*Keeping only cities from DADS that have changed boundaries
use "DADS_Est_depcom_codes_mod", clear
merge 1:1 depcom year using "coverage_broadband_all_ml"
keep if _merge == 1
drop  _merge
keep depcom
duplicates drop
gen matchindic = 0
merge 1:1 depcom using "list DADS mismatch in mvtcommune2021depcom"
keep if _merge==3
drop  _merge
*Reassigning them all the years
merge 1:m depcom using "DADS_Est_depcom_codes_mod", nogen

*Manually assigning new depcom: see details at bottom of do-file
gen depcom2=0

*Cases of firms using old depcom rather than new depcom following a merger/split/etc...
replace depcom2 = 2738 if depcom == 2630
replace depcom2 = 14697 if depcom == 14624
replace depcom2 = 35206 if depcom == 35073
replace depcom2 = 50129 if depcom == 50383
replace depcom2 = 59350 if depcom == 59355
replace depcom2 = 61483 if depcom == 61022
replace depcom2 = 67242 if depcom == 67057
*Now we have 13 duplicates because other firms in the city gave the correct depcom

*For these we have coverage data for cities they later merged with, but not for the depcom they had in some years during the period
replace depcom2 = 16297 if depcom == 16159
replace depcom2 = 26091 if depcom == 26158
replace depcom2 = 30052 if depcom == 30157
replace depcom2 = 31412 if depcom == 31307
replace depcom2 = 39367 if depcom == 39524
replace depcom2 = 48027 if depcom == 48023
replace depcom2 = 49018 if depcom == 49116
replace depcom2 = 50591 if depcom == 50179
replace depcom2 = 50216 if depcom == 50303
replace depcom2 = 50090 if depcom == 50557
replace depcom2 = 54602 if depcom == 55227 
replace depcom2 = 61168 if depcom == 61004
replace depcom2 = 61136 if depcom == 61254
replace depcom2 = 65081 if depcom == 65312
replace depcom2 = 67560 if depcom == 67153
replace depcom2 = 71443 if depcom == 71211
replace depcom2 = 71101 if depcom == 71560
replace depcom2 = 79195 if depcom == 79017
replace depcom2 = 79013 if depcom == 79037
replace depcom2 = 79013 if depcom == 79305
replace depcom2 = 80369 if depcom == 80370
replace depcom2 = 85036 if depcom == 85068


*For these we have coverage data on cities they have split from
replace depcom2 = 28368 if depcom == 28159
replace depcom2 = 57460 if depcom == 57069
replace depcom2 = 64269 if depcom == 64329
replace depcom2 = 71543 if depcom == 71353

*Drop cities for which we have not found a reason to assign other depcom
drop if depcom2==0
drop depcom
rename depcom2 depcom
*Merging DADS cities with new depcom with coverage data
*Duplicates because both 79037 and 79305 merged to become 79013
merge m:1 depcom year using "coverage_broadband_all_ml"
keep if _merge==3
drop _merge


******* Obtaining final dataset with improved merge *******

append using "DADS_coverage_perfect_match"
order matchindic, last
sort depcom year

save "DADS_coverage", replace

*Voilà
*Improved merge by 235 observations

/* Detail of manual recoding

Cases of firms using old depcom rather than new depcom following a merger/split/etc...
Safe to replace depcom
2630
14624
35073
50383
59355
61022
67057


Below are special cases. We can improve on the merge by making some assumptions/approximations
We can assume that because cities become/have been related (merged or split), we can use
coverage data of one for the other. However, in rural France cities are merged to
save on administration costs, even if they are several kilometers apart. 
Such assumptions may thus be fairly strong
I have thus constructed a match indicator (matchindic) equal to 1 for a perfect match
and to 0 if it was achieved thanks to my intervention (requested by Catherine).
It can be used to perform robustness tests to see that future results hold if we exclude those cities

For the same reason using a city block level may be more appropriate than municipality 
level. Jocelyn said it could be feasible but tricky. I could discuss that with him 
if you are interested

For these we have coverage data for cities they later merged with, but not for 
the depcom they had in some years during the period
16159
26158
30157
31307
39524
48023
49116
50179
50303
50557
55227 (here change of code in 1997, we have DADS for 1996 with the 1996 code, but coverage in 1996 only for the 1997 code)
61004
61254
65312
67153
71211
71560
79017
79037
79305
80370
85068

In the email thread on June 16th Giuseppe wrote "There has been a trend of merge/consolidation
of municipalities in France over the years, so I guess they have just rolled it 
back to have a consistent panel." This could explain why there are so many instances 
of depcom of cities that merged for which we have coverage data only on the final depcom.
It could also explain why the data is so 'neat' with 13 years of data for all depcom

Let's take the example of depcom 16159. It became 16297 in 1997. As expected we 
do not have DADS data for 16159 for or after 1997, only for 1996. In DADS it is assigned 
the correct depcom each year. Now, in coverage 16297 does not change size in 1997 
despite being merged with 16159. We can thus conclude that the size of 16297 in coverage
is that of the merged city. Furthermore, there is no coverage before 2001, when both 
cities have been merged. Thus, it probably would not be an issue to change depcom 16159
16297 for 1996.

By taking a case by case approach like this we could probably safely assign the depcom
for which we have coverage data to a number of the depcom listed above.


For these we have coverage data on cities they have split from
28159
57069
57115
64329
71353


Cities in mvtcommune2021all but could not find evidence of miscoding (for example only changed name): 
5002
11137
21551
26036
26136
26354
48087
50095
51470
55208
60099
62238
69123

No solutions to mismatch for cities not in mvtcommune2021all, and for those from 
mvtcommune2021all above 

Likely to be some mismatch because there is no coverage data for some cities (around 220 dead zone cities)
*/

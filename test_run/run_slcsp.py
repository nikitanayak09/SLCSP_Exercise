#!/usr/bin/python
"""
Assignment : https://homework.adhoc.team/slcsp/
The following code in python uses Pandas DataFrames to fetch the second lowest silver plan
for a group of zipcodes(that is present in the slcsp.csv file)
The input files that are passed to this code are

ZIPS_CSV_PATH = "./input_data/zips.csv"
PLANS_CSV_PATH = "./input_data/plans.csv"
SLCSP_CSV_PATH = "./input_data/slcsp.csv"

Kindly check if the files are present in the path defined. You could set your own path too. 

OUTPUT_SLCSP_CSV_PATH = "./Output_slcsp.csv"

This file contains the output with the rates for the desired zipcodes ONLY if you don't wish to update the
original slcsp.csv file.
The program takes a user input to confirm the same

METAL_LEVEL = "Silver" 
This assigns the metal level as Silver and can be easily changed to Gold, Platinum or any other value
based on the requirements

The program runs in two ways:
-> Without any arguments : python run_slcsp.py
	This will print the zipcodes and the respective slcsp rates on the screen and ask the user input
	to either create a new file  OUTPUT_SLCSP_CSV_PATH or update the  SLCSP_CSV_PATH
	
-> With zipcodes as the arguments : python run_slcsp.py zip1 zip2 . . 
	This will print the zipcodes and the respective slcsp rates of the zipcodes passed on the screen
	and will also ask the user to input if they would like to write the full output to the
	OUTPUT_SLCSP_CSV_PATH or update the  SLCSP_CSV_PATH
	
-Use python run_slcsp.py -help to view the options
	
In both these cases, I have handled the ambiguous zips in the code. That is, if a zip is present
in more than one area, then it is considered ambiguous and the respective column is left blank. 
If any invalid zip is passed as an argument, then an error message is thrown. 

This program has been tested on Mac and Windows 10, Python 3.9.5
"""

import pandas as pd
import numpy as np
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
Declare the path variables for the input files, output file.
"""

ZIPS_CSV_PATH = "./input_data/zips.csv"
PLANS_CSV_PATH = "./input_data/plans.csv"
SLCSP_CSV_PATH = "./input_data/slcsp.csv"
OUTPUT_SLCSP_CSV_PATH = "./Output_slcsp.csv"

"""Assign the Metal Level here to the plan that you would like to get the second smallest rates for """

METAL_LEVEL = "Silver"

"""
Handling File read errors here. Pandas DataFrames are used to read the csv file
"""
try:
	with open(ZIPS_CSV_PATH) as zips_csv, open(PLANS_CSV_PATH) as plans_csv, open(SLCSP_CSV_PATH) as slcsp_csv:
			plans = pd.read_csv(plans_csv)
			zips = pd.read_csv(zips_csv)
			slcsp = pd.read_csv(slcsp_csv)
except OSError as e:
		print(e)
		print("Error : Unable to read the input files. Please re-check if the files are present\n")
		sys.exit(1)
		
		
"""
This function runs the primary function called basic_function() that has the code to find the slcsp 
values. It aslo checks for the arguments passed in the CLI , validate the input and then perform the task accordingly
"""			
def check_args(cmd_line_args):
	result = basic_function()
	if len(sys.argv) == 1:
		print(result)
		write_to_csv(result)
		
	if len(sys.argv) > 1:
		if (sys.argv[1] == "-help"):
			print("\nUsage :")
			print("Execute the SLCSC program : python run_slcsc.py")
			print("Execute the SLCSC with zipcodes as args: python run_slcsc.py zip1 zip2 ..\n")
			sys.exit()
		else:	
			for i in range(1,len(sys.argv)):
				if sys.argv[i].isdigit():
					res_index = result.index[result['zipcode']==int(sys.argv[i])].tolist()
				else:
					res_index=[]
				if not res_index or not sys.argv[i].isdigit():
					print ("\nPlease re-check the zipcode : ",sys.argv[i],"\n" )
				else:
					print(result.iloc[res_index])
			write_to_csv(result)

"""
This function asks for the user input to direct the output to the desired location.
"""					
def write_to_csv(result):
	write_to_csv = input ("\nDo you wish to write the complete output to the original SLCSP file? Enter:\n y : To write to the original SLCSP.csv\n n : To write to a separate OutputSLCSP file\n")
	if write_to_csv == 'y':
		result.to_csv(SLCSP_CSV_PATH, index=False)
		print("\nOutput file available at :",SLCSP_CSV_PATH,"\n")
		return
	if write_to_csv == 'n':
		result.to_csv(OUTPUT_SLCSP_CSV_PATH, index=False)
		print("\nOutput file available at :",OUTPUT_SLCSP_CSV_PATH,"\n")
		return
	else:
		print("\nInvalid Key Entered!\n")
		return
		
"""
This function is the primary function of the program. Here we fetch only the plans with the concerned Metal_Level
Then we reduce the zips file to extract the zips that are needed in the output. We also remove the ambiguous zips,
and finally find the nth smallest rate for the zips.
"""
def basic_function():
	
	"""
	Extract only the silver plans from the plans.csv file. METAL_LEVEL is passed here that can be used to check for any other Metal_Type as well
	Merge the slcsc csv with the zips.csv file to fetch only the zips needed in the output(instead of fetching data for all the zips)
	This reduces the computational costs before merging to fetch the rates for all the zips
	"""
	silver_plans = plans[plans['metal_level'].str.contains(METAL_LEVEL , regex=False)]
	reduced_zips = pd.merge(slcsp['zipcode'], zips, on='zipcode')

	"""Find the ambiguous zips by grouping the zipcodes and checking the rate_areas for the respective group of zips
	If multiple rate_areas point to the same zip, then it can be considered as ambiguous.
	So here, we remove the ambiguous zips from the list and fetch the rates for the zips that are not ambiguous
	"""
	remove_ambiguous_zips = reduced_zips.groupby('zipcode').rate_area.nunique() > 1
	remove_ambiguous_zips[remove_ambiguous_zips].index.to_frame()
	required_zips = pd.DataFrame(remove_ambiguous_zips).reset_index()
	required_zips = required_zips.loc[required_zips["rate_area"] != 1]
	required_zips_data = pd.merge(required_zips['zipcode'],reduced_zips,on = 'zipcode')

	"""Find the corresponding rates from the Silver plans."""
	zipcode_silver_plans_rate = pd.merge(required_zips_data,silver_plans, on=['state','rate_area'])
	
	"""In the problem statement, we also had to assign blank to the zips that had only one rate which means that there was no second
	smallest. These next statements filter out those zipcodes. That is, get only the zipcodes that have a size >1 (which means they have more than 1 rate_area)
	"""
	df=zipcode_silver_plans_rate.groupby('zipcode').size().to_frame('size')
	zipcode_silver_plans_rate_reqa = pd.DataFrame(df).reset_index()
	zipcode_silver_plans_rate_reqa = zipcode_silver_plans_rate_reqa.loc[zipcode_silver_plans_rate_reqa["size"] != 1]
	zipcode_silver_plans_rate_reqb = pd.merge(zipcode_silver_plans_rate_reqa['zipcode'],reduced_zips,on = 'zipcode')
	zipcode_silver_plans_rate_req = pd.merge(zipcode_silver_plans_rate_reqb,silver_plans, on=['state','rate_area'])


	"""Here we use the nsmallest() method to get the smallest two rates and keep only the last which is the second smallest rate for that zipcode"""
	sorted_zipcode_silver_plans_rate = zipcode_silver_plans_rate_req.groupby('zipcode')['rate'].nsmallest(2).reset_index().drop('level_1',1).drop_duplicates(subset=['zipcode'],keep='last')


	"""In the last step, we merge the zipcodes(from the output list) with the sorted_zipcode_silver_plans_rate to get the final desired data.
	Here I have also formatted the output to include blanks and display the decimal values to two places."""
	final_result = pd.merge(slcsp['zipcode'],sorted_zipcode_silver_plans_rate[['zipcode','rate']], on = 'zipcode',how='left')
	final_result.fillna(' ', inplace=True)
	#final_result.index = np.arange(1, len(final_result)+1)
	pd.options.display.float_format = "{:,.2f}".format
	return final_result

if __name__ == "__main__":
	#basic_function()
	check_args(sys.argv)
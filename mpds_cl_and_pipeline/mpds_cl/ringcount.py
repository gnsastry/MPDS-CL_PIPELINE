import re
#####################################################################################################################################################
#**********************************************************************************************
def rc(smiles):
	global count1
	global newstr
	TC=0
	if re.search(r'([\d]*[A-Za-z+-@]+[\d]*\])',smiles):
		newstr=re.sub(r'[\d]*[A-Za-z+-@]+[\d+-]*\]','!',smiles,count=0)
		count1=0
		for j in newstr:
			if(j.isdigit()):
				count1=count1+1
		if (count1 > 0):
			TC=count1 // 2
		return TC
	elif re.search(r'[A-Za-z]+',smiles):
		newstr=re.sub(r'[\d]*[A-Za-z+-@]+[\d+-]*\]','!',smiles,count=0)
		count1=0
		for j in newstr:
			if(j.isdigit()):
				count1=count1+1
		if (count1 > 0):
			TC=count1 // 2
		return TC
###############################################################################################

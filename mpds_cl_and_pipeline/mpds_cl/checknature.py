import re
#####################################################################################################################################################
def isRingAromatic(mol, bondRing):
        for id in bondRing:
            if not mol.GetBondWithIdx(id).GetIsAromatic():
                return False
        return True
#####################################################################################################################################################
#Check the aliphatic or aromatic or mixed type of molecules
def checknature(m,z,result3,result4,result5,result6,result7,result8):
	ri = m.GetRingInfo()
	pure_arom=0
	pure_alip=0
	mixed_arom=0
#####################################################################################################################################
	if isRingAromatic(m, ri.BondRings()[0]) == True:
#Pure-arom
		if len(result3)>=1 and len(result8)==0:
			pure_arom=1
		if len(result4)>=1 and len(result8)==0:
			pure_arom=1
		if re.search(r'(c)',z) and re.search('^((?!C|N|O|P|S).)*$',z) and len(result8)==0:
			pure_arom=1
		if len(result5)>=1 and len(result8)==0:
			pure_arom=1
#Mixed-arom
		if len(result3)>=1 and len(result8)>=1:
			mixed_arom=1
#			print("Found mixed ring")
		if len(result4)>=1 and len(result8)>=1:	
			mixed_arom=1
#			print("Found mixed ring")
#Pure-ali
		if len(result3)==0 and len(result4)==0 and len(result6)>=0 and len(result7)>=0 and len(result8)>=1 and re.search('^((?!c|n|o|s|p).)*$',z):
			pure_alip=1
	else:
		#Pure-arom
		if len(result3)>=1 and len(result8)==0:
			pure_arom=1
		if len(result4)>=1 and len(result8)==0:
			pure_arom=1
		if re.search(r'(c)',z) and re.search('^((?!C|N|O|P|S).)*$',z) and len(result8)==0:
			pure_arom=1
		if len(result5)>=1 and len(result8)==0:
			pure_arom=1
#Mixed-arom
		if len(result3)>=1 and len(result8)>=1:
			mixed_arom=1
			print("Found mixed ring")
		if len(result4)>=1 and len(result8)>=1:
			mixed_arom=1
			print("Found mixed ring")
#Pure-ali
		if len(result3)==0 and len(result4)==0 and len(result6)>=0 and len(result7)>=0 and len(result8)>=1 and re.search('^((?!c|n|o|s|p).)*$',z):
			pure_alip=1
	return pure_alip,pure_arom,mixed_arom

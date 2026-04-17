import re
import rdkit as rdkit
from rdkit import Chem

def checknatureforfrag(m,frag,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res):
	pure_arom=0
	pure_alip=0
	mixed_arom_one=0
	mixed_arom_two=0
	ssr = Chem.GetSymmSSSR(m)
    # Loop over each ring and print its properties
	for i, ring_atoms in enumerate(ssr):
		three_ring_type = 0
		four_ring_type = 0
		five_ring_type = 0
		six_ring_type = 0
		ge_seven_ring_type = 0
		three_mem_aro=0
		four_mem_aro=0
		five_mem_aro=0
		six_mem_aro=0
		ge_seven_aro=0
		ring_size = len(ring_atoms)
		if ring_size == 3:
			three_ring_type = 1
		if ring_size == 4:
			four_ring_type = 1
		if ring_size == 5:
			five_ring_type = 1
			# print("found 5")
		if ring_size == 6:
			six_ring_type = 1
		if ring_size >= 7:
			ge_seven_ring_type = 1
		else:
			ring_type = 'multi-membered ring'
        # Loop through the rings and check if they are saturated, unsaturated, or aromatic
		aromatic = all([m.GetAtomWithIdx(atom).GetIsAromatic() for atom in ring_atoms])
		if aromatic:
			if three_ring_type == 1:
				three_mem_aro=1
			if four_ring_type == 1:
				four_mem_aro=1
			if five_ring_type == 1:
				five_mem_aro=1
			if six_ring_type == 1:
				six_mem_aro=1
			if ge_seven_ring_type == 1:
				ge_seven_aro=1
#Pure-arom
	if len(result3)>=1 and len(result8)==0:
		pure_arom=1
	if len(result4)>=1 and len(result8)==0:
		pure_arom=1
	if re.search(r'(c)',frag) and re.search('^((?!C|N|O|P|S).)*$',frag) and len(result8)==0:
		pure_arom=1
	if re.search(r'(c|n|o|p|s)',frag) and re.search('^((?!C|N|O|P|S).)*$',frag):
		pure_arom=1
	if len(result5)>=1 and len(result8)==0:
		pure_arom=1
#Mixed-arom
	if len(result3)>=1 and len(result8)>=1:
		print("found mixed in check_for_frag function")
		if len(res_fivear)==5 and len(res_sixar)==0:
			mixed_arom_one=1
		if len(res_fivear)==0 and len(res_sixar)==6:
			mixed_arom_one=1
		if three_mem_aro==1 and four_mem_aro==0 and five_mem_aro==0 and six_mem_aro==0 and ge_seven_aro==0:
			mixed_arom_one=1
		if three_mem_aro==0 and four_mem_aro==1 and five_mem_aro==0 and six_mem_aro==0 and ge_seven_aro==0:
			mixed_arom_one=1
		if three_mem_aro==0 and four_mem_aro==0 and five_mem_aro==1 and six_mem_aro==0 and ge_seven_aro==0:
			mixed_arom_one=1
		if three_mem_aro==0 and four_mem_aro==0 and five_mem_aro==0 and six_mem_aro==1 and ge_seven_aro==0:
			mixed_arom_one=1
		if three_mem_aro==0 and four_mem_aro==0 and five_mem_aro==0 and six_mem_aro==0 and ge_seven_aro==1:
			mixed_arom_one=1
		if len(res_fivear)==5 and len(res_sixar)==6:
			mixed_arom_two=1
		if len(res_fivear)==4 and len(res_sixar)==5:
			mixed_arom_two=1
		if len(res_fivear)==5 and len(res_sixar)>=6:
			mixed_arom_two=1
		if len(res_fivear)==5 and len(res_sixar)==4:
			mixed_arom_two=1
		if len(res_sixar)==10:
			mixed_arom_two=1
		if len(res_sixar)==12:
			mixed_arom_two=1
		if len(res_fivear)==8:
			mixed_arom_two=1
		if len(res_fivear)==10:
			mixed_arom_two=1
	if len(result4)>=1 and len(result8)>=1 and re.search('^((C|N|O|P|S).)*$',frag):
		print("found mixed in check_for_frag function")
		if len(res_fivear)==5 and len(res_sixar)==0:
			mixed_arom_one=1
		if len(res_fivear)==5 and len(res_sixar)==6:
			mixed_arom_two=1
#Pure-ali
	if len(result3)==0 and len(result4)==0 and len(result6)>=0 and len(result7)>=0 and len(result8)>=1 and re.search('^((?!c|n|o|s|p).)*$',frag):
		pure_alip=1
#Pure-arom for the pure fused
	if len(result3)>=1 and len(result8)==0 and len(fusedpat_res)==4 and len(conpat_res)==0:
		pure_arom=1
	if len(result4)>=1 and len(result8)==0 and len(fusedpat_res)==4 and len(conpat_res)==0:
		pure_arom=1
	if re.search(r'(c)',frag) and re.search('^((?!C|N|O|P|S).)*$',frag) and len(result8)==0 and len(fusedpat_res)==4 and len(conpat_res)==0:
		pure_arom=1
	if re.search(r'(c|n|o|p|s)',frag) and re.search('^((?!C|N|O|P|S).)*$',frag) and len(fusedpat_res)==4 and len(conpat_res)==0:
		pure_arom=1
	if len(result5)>=1 and len(result8)==0:
		pure_arom=1
#Mixed-arom
	if len(result3)>=1 and len(result8)>=1 and re.search('^((C|N|O|P|S).)*$',frag) and len(fusedpat_res)==4 and len(conpat_res)==0:
		if len(res_fivear)==5 and len(res_sixar)==4:
			mixed_arom_two=1
		if len(res_fivear)==2 and len(res_sixar)==6:
			mixed_arom_two=1
		if len(res_fivear)==4 and len(res_sixar)==12:
			mixed_arom_two=1
		if len(res_fivear)==5 and len(res_sixar)==8:
			mixed_arom_two=1
	if len(result4)>=1 and len(result8)>=1 and re.search('^((C|N|O|P|S).)*$',frag) and len(fusedpat_res)==4 and len(conpat_res)==0:	
		if len(res_fivear)==5 and len(res_sixar)==4:
			mixed_arom_two=1
		if len(res_fivear)==2 and len(res_sixar)==6:
			mixed_arom_two=1
		if len(res_fivear)==4 and len(res_sixar)==12:
			mixed_arom_two=1
		if len(res_fivear)==5 and len(res_sixar)==8:
			mixed_arom_two=1
#Pure-ali
	if len(result3)==0 and len(result4)==0 and len(result6)>=0 and len(result7)>=0 and len(result8)>=1 and re.search('^((?!c|n|o|s|p).)*$',frag) and len(fusedpat_res)>=4 and len(conpat_res)==0:
		pure_alip=1
	return pure_alip,pure_arom,mixed_arom_one,mixed_arom_two
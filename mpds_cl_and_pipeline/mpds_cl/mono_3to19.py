import re
from openbabel import pybel
import rdkit as rdkit
from rdkit import Chem

from . import ringcount as rc
from . import cleansmiles as cs

def checkmono(bis,bis2,smiles,fp_list,mymol,m,conpat,conpat_res):
	ssr = Chem.GetSymmSSSR(m)
    # Loop over each ring and print its properties
	for i, ring_atoms in enumerate(ssr):
		three_ring_type = 0
		four_ring_type = 0
		five_ring_type = 0
		six_ring_type = 0
		ge_seven_ring_type = 0
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
		three_mem_aro=0
		four_mem_aro=0
		five_mem_aro=0
		six_mem_aro=0
		ge_seven_aro=0
		if aromatic:
			if three_ring_type == 1:
				fp_list[3] = '1'
				three_mem_aro=1
				print("The 3 membered ring is aromatic")
			if four_ring_type == 1:
				fp_list[5] = '1'
				four_mem_aro=1
				print("The 4 membered ring is aromatic")
			if five_ring_type == 1:
				fp_list[8] = '1'
				five_mem_aro=1
				print("The 5 membered ring is aromatic")
			elif six_ring_type == 1:
				fp_list[14] = '1'
				six_mem_aro=1
				print("The 6 membered ring is aromatic")
			elif ge_seven_ring_type == 1:
				fp_list[18] = '1'
				ge_seven_aro=1
				print("The macro membered ring is aromatic")
		num_single_bonds = 0
		num_double_bonds = 0
		for j in range(ring_size):
			bond = m.GetBondBetweenAtoms(ring_atoms[j], ring_atoms[(j + 1) % ring_size])
			if bond.GetBondType() == Chem.BondType.SINGLE:
				num_single_bonds += 1
			if bond.GetBondType() == Chem.BondType.DOUBLE:
				num_double_bonds += 1
		if num_single_bonds >= 1:
			if three_ring_type == 1:
				fp_list[2] = '1'
			if four_ring_type == 1:
				fp_list[4] = '1'
			if five_ring_type == 1:
				fp_list[6] = '1'
			if six_ring_type == 1:
				fp_list[12] = '1'
			if ge_seven_ring_type == 1:
				fp_list[17] = '1'
		if num_double_bonds == 1:
			if three_ring_type == 1:
				fp_list[3] = '1'
			if four_ring_type == 1:
				fp_list[5] = '1'
			if five_ring_type == 1:
				fp_list[7] = '1'
			if six_ring_type == 1:
				fp_list[13] = '1'
			if ge_seven_ring_type == 1:
				fp_list[18] = '1'
		if num_double_bonds ==2 and six_ring_type==1:
				fp_list[13]='1'
		if num_double_bonds >= 2 and ring_size >= 7:
			if ge_seven_ring_type==1:
				fp_list[18]='1'
			
#Check if none of the BITS are 1
#	check_2to13="".join([e for e in fp_list[1:12]])
#	if check_2to13.find('1')==-1:
#		Three_mem_sat_ring=pybel.Smarts("[R]1[R][R]1")
#		Three_mem_sat_ring2=pybel.Smarts("[#6;r3]!=[#6;r3]")
	Three_mem_sat_ring=pybel.Smarts("[CH2]1[CH2][CH2]1")
	Three_mem_sat_ring2=pybel.Smarts("[!a;R;X4]1@[!a;R;X4]@[!a;R;X4]1")
	Three_mem_sat_ring3=pybel.Smarts("[!a;#6;r3]=[!a;#6;r3]")
	Four_mem_sat_ring=pybel.Smarts("[CH2]1[CH2][CH2][CH2]1")
	Four_mem_sat_ring2=pybel.Smarts("[!a;R;X4]1@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]1")
	Four_mem_sat_ring3=pybel.Smarts("[!a;#6;r4]=[!a;#6;r4]")
#		Four_mem_sat_ring=pybel.Smarts("[R]1[R][R][R]1")
#		Four_mem_sat_ring2=pybel.Smarts("[#6;r4]!=[#6;r4][r4!a]")
#		Five_mem_sat_ring=pybel.Smarts("[A;!a;R1]1[A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1]1")
	Five_mem_sat_ring=pybel.Smarts("[CH2]1[CH2][CH2][CH2][CH2]1")
	Five_mem_sat_ring2=pybel.Smarts("[!a;R;X4]1@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]1")
	Five_mem_sat_ring3=pybel.Smarts("[!a;#6;r5]=[!a;#6;r5]")
	Six_mem_sat_ring=pybel.Smarts("[CH2]1[CH2][CH2][CH2][CH2][CH2]1")
	Six_mem_sat_ring2=pybel.Smarts("[!a;R;X4]1@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]@[!a;R;X4]1")
	Six_mem_sat_ring3=pybel.Smarts("[!a;#6;r6]=[!a;#6;r6]")
#		Six_mem_sat_ring=pybel.Smarts("[A;!a;R1]1!=[A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1]1")
#		Six_mem_sat_ring2=pybel.Smarts("[#6;r6]!=[#6;r6]")
	macro_mem_sat_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6][#6]!=[#6]")
	Three_mem_unsat_ring=pybel.Smarts("[r3][A;r3]=,#[A;r3]")
	Four_mem_unsat_ring=pybel.Smarts("[r4][A;r4]=,#[A;r4]")
	Five_mem_unsat_ring=pybel.Smarts("[r5][A;r5]=,#[A;r5]")
	Six_mem_unsat_ring=pybel.Smarts("[r6][A;r6]=,#[A;r6]")
	macro_mem_unsat_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6][A;R]=,#[A;R]")
	Five_mem_arom_ring1=pybel.Smarts("a:1:a:a:a:a:1")
	Six_mem_arom_ring1=pybel.Smarts("a:1:a:a:a:a:a:1")
	Four_mem_arom_ring=pybel.Smarts("[r4&a]")
	Five_mem_arom_ring2=pybel.Smarts("[r5&a]")
	Six_mem_arom_ring2=pybel.Smarts("[r6&a]")
	macro_mem_arom_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6;a]")
######################################################################
	three_sat_res=Three_mem_sat_ring.findall(mymol)
	four_sat_res=Four_mem_sat_ring.findall(mymol)
#		five_sat_res=Five_mem_sat_ring.findall(mymol)
	six_sat_res=Six_mem_sat_ring.findall(mymol)
	macro_sat_res=macro_mem_sat_ring.findall(mymol)
	three_sat_res2=Three_mem_sat_ring2.findall(mymol)
	three_sat_res3=Three_mem_sat_ring3.findall(mymol)
	four_sat_res2=Four_mem_sat_ring2.findall(mymol)
	four_sat_res3=Four_mem_sat_ring3.findall(mymol)
	five_sat_res=Five_mem_sat_ring.findall(mymol)
	five_sat_res2=Five_mem_sat_ring2.findall(mymol)
	five_sat_res3=Five_mem_sat_ring3.findall(mymol)
	six_sat_res2=Six_mem_sat_ring2.findall(mymol)
	six_sat_res3=Six_mem_sat_ring3.findall(mymol)
######################################################################
	three_unsat_res=Three_mem_unsat_ring.findall(mymol)
	four_unsat_res=Four_mem_unsat_ring.findall(mymol)
	five_unsat_res=Five_mem_unsat_ring.findall(mymol)
	six_unsat_res=Six_mem_unsat_ring.findall(mymol)
	macro_unsat_res=macro_mem_unsat_ring.findall(mymol)
######################################################################
	four_arom_res=Four_mem_arom_ring.findall(mymol)
	five_arom_res1=Five_mem_arom_ring1.findall(mymol)
	six_arom_res1=Six_mem_arom_ring1.findall(mymol)
	macro_arom_res=macro_mem_arom_ring.findall(mymol)
	five_arom_res2=Five_mem_arom_ring2.findall(mymol)
	six_arom_res2=Six_mem_arom_ring2.findall(mymol)
######################################################################
#Class3
	if (len(three_sat_res)>=1):
		fp_list[2]='1'
	if (len(three_sat_res2)>=1):
		fp_list[2]='1'
#Class 4
	if (len(three_unsat_res)>=1):
		fp_list[3]='1'
#Class 5
	if (len(four_sat_res)>=1):
		fp_list[4]='1'
	if (len(four_sat_res2)>=1):
		fp_list[4]='1'
#Class 6
	if (len(four_unsat_res)>=1):
		fp_list[5]='1'
#Class 7
	if (len(five_sat_res)>=1): 
		fp_list[6]='1'
	if len(five_sat_res2)>=1: 
		fp_list[6]='1'
#Class 8
	if (len(five_unsat_res)>=1):
		fp_list[7]='1'
#Class 9
	if len(five_arom_res1)>=1:
		fp_list[8]='1'
	if len(five_arom_res2)==5:
		fp_list[8]='1'
#Class 13
	if (len(six_sat_res)>=1):
		fp_list[12]='1'
	if (len(six_sat_res2)>=1):
		fp_list[12]='1'
#Class 14
	if (len(six_unsat_res)>=1):
		fp_list[13]='1'
#Class 15
	if len(six_arom_res1)>=1:
		fp_list[14]='1'
	if len(six_arom_res2)==6:
		fp_list[14]='1'
#Class 18
	if (len(macro_sat_res)>=1):
		fp_list[17]='1'
#Class 19
	if (len(macro_unsat_res)>=1):
		fp_list[18]='1'
#For macrocyclic-aromatic rings whose bit is class 41
	if len(macro_arom_res)>=1:
		fp_list[18]='1'
#Patterns for special classes
	benzene=pybel.Smarts("c1ccccc1")
	benzene_isolated=pybel.Smarts("[cR1]1[cR1][cR1][cR1][cR1][cR1]1")
	pyrrole=pybel.Smarts("c1ccnc1")
	pyrrole_isolated=pybel.Smarts("[cR1]1[cR1][cR1][nR1][cR1]1")
	pyridine=pybel.Smarts("c1ccncc1")
	pyridine_isolated=pybel.Smarts("[cR1]1[cR1][cR1][nR1][cR1][cR1]1")
	furan=pybel.Smarts("c1ccoc1")
	furan_isolated=pybel.Smarts("[cR1]1[cR1][cR1][oR1][cR1]1")
	thiophene=pybel.Smarts("c1ccsc1")
	thiophene_isolated=pybel.Smarts("[cR1]1[cR1][cR1][sR1][cR1]1")
	fused=pybel.Smarts("[R2]")
	conpat=pybel.Smarts("[R]!@[R]")
#########################################################################################
	ringcount=rc.rc(smiles)
	benzene_res=benzene.findall(mymol)
	pyrrole_res=pyrrole.findall(mymol)
	pyridine_res=pyridine.findall(mymol)
	furan_res=furan.findall(mymol)
	thiophene_res=thiophene.findall(mymol)
	fused_res=fused.findall(mymol)
	res_benzene_neighbour=benzene_isolated.findall(mymol)
	res_pyrrole_neighbour=pyrrole_isolated.findall(mymol)
	res_furan_neighbour=furan_isolated.findall(mymol)
	res_thiophene_neighbour=thiophene_isolated.findall(mymol)
	res_pyridine_neighbour=pyridine_isolated.findall(mymol)
#########################################################################################
	if len(fused_res)==0:
		if len(benzene_res)==1:
			fp_list[15]='1'
		if len(pyrrole_res)==1:
			fp_list[9]='1'
		if len(pyridine_res)==1:
			fp_list[16]='1'
		if len(furan_res)==1:
			fp_list[10]='1'
		if len(thiophene_res)==1:
			fp_list[11]='1'		
	if len(fused_res)!=0 and len(conpat_res)>=1:
		if len(res_benzene_neighbour)>=1:
	 		fp_list[15]='1'
		if len(res_pyrrole_neighbour)==1:
	 		fp_list[9]='1'
		if len(res_pyridine_neighbour)==1:
	 		fp_list[16]='1'
		if len(res_furan_neighbour)==1:
	 		fp_list[10]='1'
		if len(res_thiophene_neighbour)==1:
	 		fp_list[11]='1'
	if (len(bis2)>0):
		if len(fused_res)==0:
			if len(benzene_res)==1:
				fp_list[15]='1'
			if len(pyrrole_res)==1:
				fp_list[9]='1'
			if len(pyridine_res)==1:
				fp_list[16]='1'
			if len(furan_res)==1:
				fp_list[10]='1'
			if len(thiophene_res)==1:
				fp_list[11]='1'
	if len(fused_res)!=0 and len(conpat_res)>=1:
	 	if len(res_benzene_neighbour)==1:
	 		fp_list[15]='1'
	 	if len(res_pyrrole_neighbour)==1:
	 		fp_list[9]='1'
	 	if len(res_pyridine_neighbour)==1:
	 		fp_list[16]='1'
	 	if len(res_furan_neighbour)==1:
	 		fp_list[10]='1'
	 	if len(res_thiophene_neighbour)==1:
	 		fp_list[11]='1'
	if (len(bis)>0):
		print("entered bis loop")
		bs=[]
		labels=[]
		for bi in bis:
			b = m.GetBondBetweenAtoms(bi[0],bi[1])
			if b.GetBeginAtomIdx()==bi[0]:
				labels.append((10,1))
			else:
				labels.append((1,10))
			bs.append(b.GetIdx())
		nm = Chem.FragmentOnBonds(m,bs,dummyLabels=labels)
		frag = Chem.MolToSmiles(nm,True)
	#Fragmented frags
		fment=frag.split(".")
		nRings=[]
		for fragment in fment:
			corrected_smiles=cs.remove_non_alphabetic(fragment)
			largest=0
			fragmentcount=rc.rc(corrected_smiles)
			if fragmentcount > largest:
			# 	print("This is the fragmented frag",fragment)
#				new_mol=Chem.MolFromSmiles(fragment)
				fragment = re.sub(r'\[al.*?\]', '[Al]', fragment, re.IGNORECASE)
				new_frag=pybel.readstring("smi",fragment)
#Compute for openbabel patterns
				benzene_res=benzene.findall(new_frag)
				pyrrole_res=pyrrole.findall(new_frag)
				pyridine_res=pyridine.findall(new_frag)
				furan_res=furan.findall(new_frag)
				conpat_res=conpat.findall(new_frag)
				thiophene_res=thiophene.findall(new_frag)
				fused_res=fused.findall(new_frag)
				res_benzene_neighbour=benzene_isolated.findall(new_frag)
				res_pyrrole_neighbour=pyrrole_isolated.findall(new_frag)
				res_furan_neighbour=furan_isolated.findall(new_frag)
				res_thiophene_neighbour=thiophene_isolated.findall(new_frag)
				res_pyridine_neighbour=pyridine_isolated.findall(new_frag)
###############################################################################################################
				if len(fused_res)==0 and len(conpat_res)>=0:
					if len(benzene_res)>=1:
						fp_list[15]='1'
					if len(pyrrole_res)>=1:
						fp_list[9]='1'
					if len(pyridine_res)>=1:
						fp_list[16]='1'
					if len(furan_res)>=1:
						fp_list[10]='1'
					if len(thiophene_res)>=1:
						fp_list[11]='1'
				if len(fused_res)!=0 and len(conpat_res)>=1:
					if len(res_benzene_neighbour)>=1:
						fp_list[15]='1'
					if len(res_pyrrole_neighbour)==1:
				 		fp_list[9]='1'
					if len(res_pyridine_neighbour)==1:
				 		fp_list[16]='1'
					if len(res_furan_neighbour)==1:
				 		fp_list[10]='1'
					if len(res_thiophene_neighbour)==1:
				 		fp_list[11]='1'
	if len(bis2)==0:
		if len(fused_res)==0:
			if len(benzene_res)==1:
				fp_list[15]='1'
			if len(pyrrole_res)==1:
				fp_list[9]='1'
			if len(pyridine_res)==1:
				fp_list[16]='1'
			if len(furan_res)==1:
				fp_list[10]='1'
			if len(thiophene_res)==1:
				fp_list[11]='1'
		if len(fused_res)!=0 and len(conpat_res)>=1:
		 	if len(res_benzene_neighbour)==1:
		 		fp_list[15]='1'
		 	if len(res_pyrrole_neighbour)==1:
		 		fp_list[9]='1'
		 	if len(res_pyridine_neighbour)==1:
		 		fp_list[16]='1'
		 	if len(res_furan_neighbour)==1:
		 		fp_list[10]='1'
		 	if len(res_thiophene_neighbour)==1:
		 		fp_list[11]='1'
	return fp_list

import re
from openbabel import pybel
import rdkit as rdkit
from rdkit import Chem

from . import ringcount as rc
from . import cleansmiles as cs
################################################################################################
fused=pybel.Smarts("[R2]")
conpat=pybel.Smarts("[R]!@[R]")
################################################################################################
three_and_others=pybel.Smarts("[r3][R2][r]")
four_and_others=pybel.Smarts("[r4][R2][r]")
################################################################################################
Five_mem_nonarom_ring1=pybel.Smarts("[A;!a;R1]1[A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1]1")
Five_mem_nonarom_ring2=pybel.Smarts("[r5&!a]")
################################################################################################
Six_mem_nonarom_ring1=pybel.Smarts("[A;!a;R1]1[A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1][A;!a;R1]1")
Six_mem_nonarom_ring2=pybel.Smarts("[r6&!a]")
################################################################################################
Five_mem_arom_ring1=pybel.Smarts("a:1:a:a:a:a:1")
Five_mem_arom_ring2=pybel.Smarts("[r5&a]")
################################################################################################
Six_mem_arom_ring=pybel.Smarts("a:1:a:a:a:a:a:1")
Six_mem_arom_ring2=pybel.Smarts("[r6&a]")
Six_mem_ring2=pybel.Smarts("[r6]")
################################################################################################
macro_mem_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6]")
macro_mem_nonarom_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6;!a]")
macro_mem_arom_ring=pybel.Smarts("[r;!r3;!r4;!r5;!r6;a]")
geseven_and_geseven=pybel.Smarts("[r;!r3!r4!r5!r6][R2][r;!r3!r4!r5!r6]")
################################################################################################
indole=pybel.Smarts("c1cc2ccccc2n1")
################################################################################################
def checkbi(bis,bis2,smiles,fp_list,mymol,m):
	ringcount=rc.rc(smiles)
	mymol=pybel.readstring("smi",smiles)
	res_fused=fused.findall(mymol)
	res_conpat=conpat.findall(mymol)
	res_three_and_others=three_and_others.findall(mymol)
	res_four_and_others=four_and_others.findall(mymol)
	res_five_mem_nonarom_ring1=Five_mem_nonarom_ring1.findall(mymol)
	res_five_mem_nonarom_ring2=Five_mem_nonarom_ring2.findall(mymol)
	res_six_mem_nonarom_ring1=Six_mem_nonarom_ring1.findall(mymol)
	res_six_mem_nonarom_ring2=Six_mem_nonarom_ring2.findall(mymol)
	res_five_mem_arom_ring1=Five_mem_arom_ring1.findall(mymol)
	res_five_mem_arom_ring2=Five_mem_arom_ring2.findall(mymol)
	res_six_mem_arom_ring1=Six_mem_arom_ring.findall(mymol)
	res_six_mem_arom_ring2=Six_mem_arom_ring2.findall(mymol)
	res_six_mem_ring2=Six_mem_ring2.findall(mymol)
	res_macro_mem_ring=macro_mem_ring.findall(mymol)
	res_macro_mem_nonarom_ring=macro_mem_nonarom_ring.findall(mymol)
	res_macro_mem_arom_ring=macro_mem_arom_ring.findall(mymol)
	res_geseven_and_geseven=geseven_and_geseven.findall(mymol)
	res_indole=indole.findall(mymol)
###############################################################################################################
	three_and_others_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r3][R2][r;!r3]"))
	four_and_others_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r4][R2][r;!r3!r4]"))
	five_mem_nonarom_ring2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r5&!a]"))
	six_mem_nonarom_ring2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r6&!a]"))
	five_mem_arom_ring2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r5&a]"))
	six_mem_arom_ring2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r6&a]"))
	six_mem_ring2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r6]"))
	macro_mem_ring_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6]"))
	macro_mem_nonarom_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6;!a]"))
	macro_mem_arom_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6;a]"))
	geseven_and_geseven_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6][R2][r;!r3;!r4;!r5;!r6]"))
	fused_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[R2]"))
	conpat_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[R]!@[R]"))
#################################################################################################################
	if ringcount==2:
	#Class21
		if len(res_conpat)==1:
			fp_list[20]='1'
	#Class22
		if len(res_three_and_others)>=1 and len(res_fused)>=1:
			fp_list[21]='1'
	#Class23
		if len(res_four_and_others)>=1 and len(res_fused)>=2:
			fp_list[22]='1'
	# #class24
		if len(res_five_mem_arom_ring2)==8 and len(res_fused)>=2:
			fp_list[23]='1'
	#class25
		if len(res_five_mem_arom_ring2)==5 and len(res_five_mem_nonarom_ring2)==3 and len(res_fused)>=2:
			fp_list[24]='1'
	#class26
		if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
			fp_list[25]='1'
	#class27
		if len(res_indole)>=1:
			fp_list[26]='1'
	#class28
		if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
			fp_list[27]='1'
	#class29
		if len(res_five_mem_nonarom_ring2)==3 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
			fp_list[28]='1'
	#class30
		if len(res_five_mem_nonarom_ring2)==8 and len(res_fused)>=2:
			fp_list[29]='1'
		if len(res_five_mem_nonarom_ring2)==7 and len(res_fused)>=2:
			fp_list[29]='1'
		if len(res_five_mem_nonarom_ring2)==5 and len(res_six_mem_nonarom_ring2)==6 and len(res_fused)>=2:
			fp_list[29]='1'
		if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=7 and len(res_fused)>=2:
			fp_list[29]='1'
		if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
			fp_list[29]='1'
	#Class31
		if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_arom_ring)>=5 and len(res_fused)>=2:
			fp_list[30]='1'
		if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
			fp_list[30]='1'	
	#class32
		if len(res_six_mem_arom_ring2)==10 and len(res_fused)>=2:
			fp_list[31]='1'
	#class33
		if len(res_six_mem_arom_ring2)==6 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
			fp_list[32]='1'
	#class34
		if len(res_six_mem_nonarom_ring2)==10 and len(res_fused)>=2:
			fp_list[33]='1'
		if len(res_six_mem_nonarom_ring2)==9 and len(res_fused)>=2:
			fp_list[33]='1'
		if len(res_six_mem_nonarom_ring2)==8 and len(res_fused)>=2:
			fp_list[33]='1'
	#class35
		if len(res_six_mem_ring2)>=1 and len(res_macro_mem_ring)>=1 and len(res_fused)>=2:
			fp_list[34]='1'
	#class36
		if len(res_geseven_and_geseven)>=2 and len(res_fused)>=2:
			fp_list[35]='1'
	#class37
		if len(res_fused)==1:
			fp_list[36]='1'
############################################################################# Exception handling through rdkit **************************************************************
# 	#class14
# 		if len(conpat_rdk)==1:
# 			fp_list[13]='1'
# 	#Class15
# 		if len(three_and_others_rdk)>=1 and len(fused_rdk)>=1:
# 			fp_list[20]='1'
# 	#Class16
# 		if len(four_and_others_rdk)>=1 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 	#class17
# 		if len(five_mem_arom_ring2_rdk)==8 and len(fused_rdk)>=2:
# 			fp_list[22]='1'
# 	#class18
# 		if len(five_mem_arom_ring2_rdk)==5 and len(five_mem_nonarom_ring2_rdk)==3 and len(fused_rdk)>=2:
# 			fp_list[23]='1'
# 	#class19
# 		if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
# 			fp_list[24]='1'
# 	#class20
# 		if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
# 			fp_list[20]='1'
# 	#class21
# 		if len(res_five_mem_nonarom_ring2)==3 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
# 			fp_list[20]='1'
# 	#class22
# 		if len(five_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 		if len(five_mem_nonarom_ring2_rdk)==7 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==6 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=7 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
# 			fp_list[21]='1'
# 	#class23
# 		if len(six_mem_arom_ring2_rdk)==10 and len(fused_rdk)>=2:
# 			fp_list[22]='1'
# 	#class24
# 		if len(six_mem_arom_ring2_rdk)==6 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
# 			fp_list[23]='1'
# 	#class25
# 		if len(six_mem_nonarom_ring2_rdk)==10 and len(fused_rdk)>=2:
# 			fp_list[24]='1'
# 		if len(six_mem_nonarom_ring2_rdk)==9 and len(fused_rdk)>=2:
# 			fp_list[24]='1'
# 		if len(six_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
# 			fp_list[24]='1'
# 	#class26
# 		if len(six_mem_ring2_rdk)>=1 and len(macro_mem_ring_rdk)>=1 and len(fused_rdk)>=2:
# 			fp_list[25]='1'
# 	#class27
# #		if len(geseven_and_geseven_rdk)>=2 and len(fused_rdk)>=2:
# #			fp_list[26]='1'
# 	#class28
# 		if len(fused_rdk)==1:
# 			fp_list[27]='1'
# 	#class56
# 		if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_arom_rdk)>=5 and len(fused_rdk)>=2:
# 				fp_list[55]='1'
# 		if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
# 				fp_list[55]='1'
############################################################# END OF FIRST CONDITIONAL LOOP *****************************************************************************		
	elif (len(bis2)>0) and len(bis)==0:
			if ringcount==2:
			#Class21
				if len(res_conpat)==1:
					fp_list[20]='1'
			#Class22
				if len(res_three_and_others)>=1 and len(res_fused)>=1:
					fp_list[21]='1'
			#Class23
				if len(res_four_and_others)>=1 and len(res_fused)>=2:
					fp_list[22]='1'
			#Class24
				if len(res_five_mem_arom_ring2)==8 and len(res_fused)>=2:
					fp_list[23]='1'
			#class25
				if len(res_five_mem_arom_ring2)==5 and len(res_five_mem_nonarom_ring2)==3 and len(res_fused)>=2:
					fp_list[24]='1'
			#class26
				if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
					fp_list[25]='1'
			#class27
				if len(res_indole)>=1:
					fp_list[26]='1'
			#class28
				if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
					fp_list[27]='1'
			#class29
				if len(res_five_mem_nonarom_ring2)==3 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
					fp_list[28]='1'
			#class30
				if len(res_five_mem_nonarom_ring2)==8 and len(res_fused)>=2:
					fp_list[29]='1'
				if len(res_five_mem_nonarom_ring2)==7 and len(res_fused)>=2:
					fp_list[29]='1'
				if len(res_five_mem_nonarom_ring2)==5 and len(res_six_mem_nonarom_ring2)==6 and len(res_fused)>=2:
					fp_list[29]='1'
				if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=7 and len(res_fused)>=2:
					fp_list[29]='1'
				if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
					fp_list[29]='1'
			#Class31
				if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_arom_ring)>=5 and len(res_fused)>=2:
					fp_list[30]='1'
				if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
					fp_list[30]='1'	
			#class32
				if len(res_six_mem_arom_ring2)==10 and len(res_fused)>=2:
					fp_list[31]='1'
			#class33
				if len(res_six_mem_arom_ring2)==6 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
					fp_list[32]='1'
			#class34
				if len(res_six_mem_nonarom_ring2)==10 and len(res_fused)>=2:
					fp_list[33]='1'
				if len(res_six_mem_nonarom_ring2)==9 and len(res_fused)>=2:
					fp_list[33]='1'
				if len(res_six_mem_nonarom_ring2)==8 and len(res_fused)>=2:
					fp_list[33]='1'
			#class35
				if len(res_six_mem_ring2)>=1 and len(res_macro_mem_ring)>=1 and len(res_fused)>=2:
					fp_list[34]='1'
			#class36
				if len(res_geseven_and_geseven)>=2 and len(res_fused)>=2:
					fp_list[35]='1'
			#class37
				if len(res_fused)==1:
					fp_list[36]='1'
################################################# Exception handling through rdkit *********************************************************
	# 		#class14
	# 			if len(conpat_rdk)==1:
	# 				fp_list[13]='1'
	# 		#Class15
	# 			if len(three_and_others_rdk)>=1 and len(fused_rdk)>=1:
	# 				fp_list[20]='1'
	# 		#Class16
	# 			if len(four_and_others_rdk)>=1 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'
	# 		#class17
	# 			if len(five_mem_arom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 				fp_list[22]='1'
	# 		#class18
	# 			if len(five_mem_arom_ring2_rdk)==5 and len(five_mem_nonarom_ring2_rdk)==3 and len(fused_rdk)>=2:
	# 				fp_list[23]='1'
	# 		#class19
	# 			if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 				fp_list[24]='1'
	# 		#class20
	# 			if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 				fp_list[20]='1'
	# 		#class21
	# 			if len(res_five_mem_nonarom_ring2)==3 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 				fp_list[20]='1'
	# 		#class22
	# 			if len(five_mem_nonarom_ring2_rdk)==7 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'
	# 			if len(five_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'					
	# 			if len(five_mem_nonarom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'
	# 			if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=7 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'
	# 			if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
	# 				fp_list[21]='1'

	# 		#class23
	# 			if len(six_mem_arom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 				fp_list[22]='1'
	# 		#class24
	# 			if len(six_mem_arom_ring2_rdk)==6 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 				fp_list[23]='1'
	# 		#class25
	# 			if len(six_mem_nonarom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 				fp_list[24]='1'
	# 			if len(six_mem_nonarom_ring2_rdk)==9 and len(fused_rdk)>=2:
	# 				fp_list[24]='1'
	# 			if len(six_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 				fp_list[24]='1'
	# 		#class26
	# 			if len(six_mem_ring2_rdk)>=1 and len(macro_mem_ring_rdk)>=1 and len(fused_rdk)>=2:
	# 				fp_list[25]='1'
	# 		#class27
	# #			if len(geseven_and_geseven_rdk)>=2 and len(fused_rdk)>=2:
	# #				fp_list[26]='1'
	# 		#class28
	# 			if len(fused_rdk)==1:
	# 				fp_list[27]='1'
	# 		#class56
	# 			if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_arom_rdk)>=5 and len(fused_rdk)>=2:
	# 					fp_list[55]='1'
	# 			if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
	# 					fp_list[55]='1'
############################################################# END OF SECOND CONDITIONAL LOOP *****************************************************************************	
	elif (len(bis)>0):
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
		#	noise.write("This is fragmented frag:"+str(fragment+"\n"))
			corrected_smiles=cs.remove_non_alphabetic(fragment)
		#	print(corrected_smiles)
		#	noise.write("This is corrected frag:"+str(corrected_smiles+"\n"))
			largest=0
			fragmentcount=rc.rc(corrected_smiles)
			if fragmentcount > largest:
#				if "[al]" in fragment:
				fragment = re.sub(r'\[al.*?\]', '[Al]', fragment, re.IGNORECASE)
				new_frag = pybel.readstring("smi",fragment)
#				new_mol=Chem.MolFromSmiles(fragment)
				res_fused=fused.findall(new_frag)
				res_conpat=conpat.findall(new_frag)
				res_three_and_others=three_and_others.findall(new_frag)
				res_four_and_others=four_and_others.findall(new_frag)
				res_five_mem_nonarom_ring1=Five_mem_nonarom_ring1.findall(new_frag)
				res_five_mem_nonarom_ring2=Five_mem_nonarom_ring2.findall(new_frag)
				res_six_mem_nonarom_ring1=Six_mem_nonarom_ring1.findall(new_frag)
				res_six_mem_nonarom_ring2=Six_mem_nonarom_ring2.findall(new_frag)
				res_five_mem_arom_ring1=Five_mem_arom_ring1.findall(new_frag)
				res_five_mem_arom_ring2=Five_mem_arom_ring2.findall(new_frag)
				res_six_mem_arom_ring1=Six_mem_arom_ring.findall(new_frag)
				res_six_mem_arom_ring2=Six_mem_arom_ring2.findall(new_frag)
				res_six_mem_ring2=Six_mem_ring2.findall(new_frag)
				res_macro_mem_ring=macro_mem_ring.findall(new_frag)
				res_macro_mem_nonarom_ring=macro_mem_nonarom_ring.findall(new_frag)
				res_macro_mem_arom_ring=macro_mem_arom_ring.findall(new_frag)
				res_geseven_and_geseven=geseven_and_geseven.findall(new_frag)
				res_indole=indole.findall(new_frag)
				if fragmentcount==2:
					if len(res_conpat)==1:
						fp_list[20]='1'
				#Class22
					if len(res_three_and_others)>=1 and len(res_fused)>=1:
						fp_list[21]='1'
				#Class23
					if len(res_four_and_others)>=1 and len(res_fused)>=2:
						fp_list[22]='1'
				#Class24
					if len(res_five_mem_arom_ring2)==8 and len(res_fused)>=2:
						fp_list[23]='1'
				#class25
					if len(res_five_mem_arom_ring2)==5 and len(res_five_mem_nonarom_ring2)==3 and len(res_fused)>=2:
						fp_list[24]='1'
				#class26
					if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
						fp_list[25]='1'
				#class27
					if len(res_indole)>=1:
						fp_list[26]='1'
				#class28
					if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
						fp_list[27]='1'
				#class29
					if len(res_five_mem_nonarom_ring2)==3 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
						fp_list[28]='1'
				#class30
					if len(res_five_mem_nonarom_ring2)==8 and len(res_fused)>=2:
						fp_list[29]='1'
					if len(res_five_mem_nonarom_ring2)==7 and len(res_fused)>=2:
						fp_list[29]='1'
					if len(res_five_mem_nonarom_ring2)==5 and len(res_six_mem_nonarom_ring2)==6 and len(res_fused)>=2:
						fp_list[29]='1'
					if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=7 and len(res_fused)>=2:
						fp_list[29]='1'
					if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
						fp_list[29]='1'
				#Class31
					if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_arom_ring)>=5 and len(res_fused)>=2:
						fp_list[30]='1'
					if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
						fp_list[30]='1'	
				#class32
					if len(res_six_mem_arom_ring2)==10 and len(res_fused)>=2:
						fp_list[31]='1'
				#class33
					if len(res_six_mem_arom_ring2)==6 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
						fp_list[32]='1'
				#class34
					if len(res_six_mem_nonarom_ring2)==10 and len(res_fused)>=2:
						fp_list[33]='1'
					if len(res_six_mem_nonarom_ring2)==9 and len(res_fused)>=2:
						fp_list[33]='1'
					if len(res_six_mem_nonarom_ring2)==8 and len(res_fused)>=2:
						fp_list[33]='1'
				#class35
					if len(res_six_mem_ring2)>=1 and len(res_macro_mem_ring)>=1 and len(res_fused)>=2:
						fp_list[34]='1'
				#class36
					if len(res_geseven_and_geseven)>=2 and len(res_fused)>=2:
						fp_list[35]='1'
				#class37
					if len(res_fused)==1:
						fp_list[36]='1'
			###############################################################################################################
	# 			three_and_others_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r3][R2][r;!r3]"))
	# 			four_and_others_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r4][R2][r;!r3!r4]"))
	# 			five_mem_nonarom_ring2_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r5&!a]"))
	# 			six_mem_nonarom_ring2_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r6&!a]"))
	# 			five_mem_arom_ring2_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r5&a]"))
	# 			six_mem_arom_ring2_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r6&a]"))
	# 			six_mem_ring2_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r6]"))
	# 			macro_mem_ring_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6]"))
	# 			macro_mem_nonarom_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6;!a]"))
	# 			macro_mem_arom_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3;!r4;!r5;!r6;a]"))
	# #			geseven_and_geseven_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[r;!r3!r4!r5!r6][R2][r;!r3!r4!r5!r6]"))
	# 			fused_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[R2]"))
	# 			conpat_rdk=Chem.MolFromSmiles(fragment).GetSubstructMatches(Chem.MolFromSmarts("[R]!@[R]"))
	
################## Handling the exceptions through rdkit ***********************************************************************************************
				#class14
	# 				if len(conpat_rdk)==1:
	# 					fp_list[13]='1'
	# 			#Class15
	# 				if len(three_and_others_rdk)>=1 and len(fused_rdk)>=1:
	# 					fp_list[20]='1'
	# 			#Class16
	# 				if len(four_and_others_rdk)>=1 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 			#class17
	# 				if len(five_mem_arom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 					fp_list[22]='1'
	# 			#class18
	# 				if len(five_mem_arom_ring2_rdk)==5 and len(five_mem_nonarom_ring2_rdk)==3 and len(fused_rdk)>=2:
	# 					fp_list[23]='1'
	# 			#class19
	# 				if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 					fp_list[24]='1'
	# 			#class20
	# 				if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 					fp_list[20]='1'
	# 			#class21
	# 				if len(res_five_mem_nonarom_ring2)==3 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 					fp_list[20]='1'
	# 			#class22
	# 				if len(five_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 				if len(five_mem_nonarom_ring2_rdk)==7 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 				if len(five_mem_nonarom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 				if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=7 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 				if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
	# 					fp_list[21]='1'
	# 			#class23
	# 				if len(six_mem_arom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 					fp_list[22]='1'
	# 			#class24
	# 				if len(six_mem_arom_ring2_rdk)==6 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 					fp_list[23]='1'
	# 			#class25
	# 				if len(six_mem_nonarom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 					fp_list[24]='1'
	# 					print("PROBLEM*****************************************")
	# 				if len(six_mem_nonarom_ring2_rdk)==9 and len(fused_rdk)>=2:
	# 					fp_list[24]='1'
	# 				if len(six_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 					fp_list[24]='1'
	# 			#class26
	# 				if len(six_mem_ring2_rdk)>=1 and len(macro_mem_ring_rdk)>=1 and len(fused_rdk)>=2:
	# 					fp_list[25]='1'
	# 			#class27
	# #				if len(geseven_and_geseven_rdk)>=2 and len(fused_rdk)>=2:
	# #					fp_list[26]='1'
	# 			#class28
	# 				if len(fused_rdk)==1:
	# 					fp_list[27]='1'
	# 			#class56
	# 				if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_arom_rdk)>=5 and len(fused_rdk)>=2:
	# 						fp_list[55]='1'
	# 				if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
	# 						fp_list[55]='1'
############################################################# END OF THIRD CONDITIONAL LOOP *****************************************************************************
	elif len(bis2)==0:
		if ringcount==2:
			if len(res_conpat)==1:
				fp_list[20]='1'
		#Class22
			if len(res_three_and_others)>=1 and len(res_fused)>=1:
				fp_list[21]='1'
		#Class23
			if len(res_four_and_others)>=1 and len(res_fused)>=2:
				fp_list[22]='1'
		#Class24
			if len(res_five_mem_arom_ring2)==8 and len(res_fused)>=2:
				fp_list[23]='1'
		#class25
			if len(res_five_mem_arom_ring2)==5 and len(res_five_mem_nonarom_ring2)==3 and len(res_fused)>=2:
				fp_list[24]='1'
		#class26
			if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
				fp_list[25]='1'
		#class27
			if len(res_indole)>=1:
				fp_list[26]='1'
		#class28
			if len(res_five_mem_arom_ring2)==5 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
				fp_list[27]='1'
		#class29
			if len(res_five_mem_nonarom_ring2)==3 and len(res_six_mem_arom_ring2)==6 and len(res_fused)>=2:
				fp_list[28]='1'
		#class30
			if len(res_five_mem_nonarom_ring2)==8 and len(res_fused)>=2:
				fp_list[29]='1'
			if len(res_five_mem_nonarom_ring2)==7 and len(res_fused)>=2:
				fp_list[29]='1'
			if len(res_five_mem_nonarom_ring2)==5 and len(res_six_mem_nonarom_ring2)==6 and len(res_fused)>=2:
				fp_list[29]='1'
			if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=7 and len(res_fused)>=2:
				fp_list[29]='1'
			if len(res_five_mem_nonarom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
				fp_list[29]='1'
		#Class31
			if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_arom_ring)>=5 and len(res_fused)>=2:
				fp_list[30]='1'
			if len(res_five_mem_arom_ring2)==5 and len(res_macro_mem_nonarom_ring)>=5 and len(res_fused)>=2:
				fp_list[30]='1'	
		#class32
			if len(res_six_mem_arom_ring2)==10 and len(res_fused)>=2:
				fp_list[31]='1'
		#class33
			if len(res_six_mem_arom_ring2)==6 and len(res_six_mem_nonarom_ring2)==4 and len(res_fused)>=2:
				fp_list[32]='1'
		#class34
			if len(res_six_mem_nonarom_ring2)==10 and len(res_fused)>=2:
				fp_list[33]='1'
			if len(res_six_mem_nonarom_ring2)==9 and len(res_fused)>=2:
				fp_list[33]='1'
			if len(res_six_mem_nonarom_ring2)==8 and len(res_fused)>=2:
				fp_list[33]='1'
		#class35
			if len(res_six_mem_ring2)>=1 and len(res_macro_mem_ring)>=1 and len(res_fused)>=2:
				fp_list[34]='1'
		#class36
			if len(res_geseven_and_geseven)>=2 and len(res_fused)>=2:
				fp_list[35]='1'
		#class37
			if len(res_fused)==1:
				fp_list[36]='1'
################################### Exception handling through rdkit **************************************************************
	# 	#class14
	# 		if len(conpat_rdk)==1:
	# 			fp_list[13]='1'
	# 	#Class15
	# 		if len(three_and_others_rdk)>=1 and len(fused_rdk)>=1:
	# 			fp_list[20]='1'
	# 	#Class16
	# 		if len(four_and_others_rdk)>=1 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 	#class17
	# 		if len(five_mem_arom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 			fp_list[22]='1'
	# 	#class18
	# 		if len(five_mem_arom_ring2_rdk)==5 and len(five_mem_nonarom_ring2_rdk)==3 and len(fused_rdk)>=2:
	# 			fp_list[23]='1'
	# 	#class19
	# 		if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 			fp_list[24]='1'
	# 	#class20
	# 		if len(five_mem_arom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 			fp_list[20]='1'
	# 	#class21
	# 		if len(res_five_mem_nonarom_ring2)==3 and len(six_mem_arom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 			fp_list[20]='1'
	# 	#class22
	# 		if len(five_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 		if len(five_mem_nonarom_ring2_rdk)==7 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(six_mem_nonarom_ring2_rdk)==6 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=7 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 		if len(five_mem_nonarom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=5 and len(fused_rdk)>=2:
	# 			fp_list[21]='1'
	# 	#class23
	# 		if len(six_mem_arom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 			fp_list[22]='1'
	# 	#class24
	# 		if len(six_mem_arom_ring2_rdk)==6 and len(six_mem_nonarom_ring2_rdk)==4 and len(fused_rdk)>=2:
	# 			fp_list[23]='1'
	# 	#class25
	# 		if len(six_mem_nonarom_ring2_rdk)==10 and len(fused_rdk)>=2:
	# 			fp_list[24]='1'
	# 		if len(six_mem_nonarom_ring2_rdk)==9 and len(fused_rdk)>=2:
	# 			fp_list[24]='1'
	# 		if len(six_mem_nonarom_ring2_rdk)==8 and len(fused_rdk)>=2:
	# 			fp_list[24]='1'
	# 	#class26
	# 		if len(six_mem_ring2_rdk)>=1 and len(macro_mem_ring_rdk)>=1 and len(fused_rdk)>=2:
	# 			fp_list[25]='1'
	# 	#class27
	# #		if len(geseven_and_geseven_rdk)>=2 and len(fused_rdk)>=2:
	# #			fp_list[26]='1'
	# 	#class28
	# 		if len(fused_rdk)==1:
	# 			fp_list[27]='1'
	# 	#class56
	# 		if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_arom_rdk)>=7 and len(fused_rdk)>=2:
	# 				fp_list[55]='1'
	# 		if len(five_mem_arom_ring2_rdk)==5 and len(macro_mem_nonarom_rdk)>=7 and len(fused_rdk)>=2:
	# 				fp_list[55]='1'
	return fp_list

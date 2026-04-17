import re

import rdkit as rdkit
from rdkit import Chem
from openbabel import pybel

from . import ringcount as rc
from . import cleansmiles as cs
from . import check_for_frag as ckf


patt3=pybel.Smarts("[a;R]")
patt4=pybel.Smarts("[n,o,s,p;R]")
patt5=pybel.Smarts("[c]:[!c]")
patt6=pybel.Smarts("[R]=[R]")
patt7=pybel.Smarts("[R]#[R]")
patt8=pybel.Smarts("[A;R]")
fivear=pybel.Smarts("[r5&a]")
sixar=pybel.Smarts("[r6&a]")
def checktr(bis,bis2,smiles,fp_list,mymol,m,fusedpat,fusedpat_res,conpat,conpat_res):
	ringcount=rc.rc(smiles)
	mymol=pybel.readstring("smi",smiles)
	result3=patt3.findall(mymol)
	result4=patt4.findall(mymol)
	result5=patt5.findall(mymol)
	result6=patt6.findall(mymol)
	result7=patt7.findall(mymol)
	result8=patt8.findall(mymol)
	res_fivear=fivear.findall(mymol)
	res_sixar=sixar.findall(mymol)
	conpat=pybel.Smarts("[R]!@[R]")
	fusedpat=pybel.Smarts("[R2]")
#For tricyclic with connected rings
	if ringcount==3 and len(conpat_res)==2 and len(fusedpat_res)==0:
		pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
		if mixed_arom_one==1:
			fp_list[37]='1'
		if mixed_arom_two==1:
			fp_list[38]='1'
		if pure_arom==1:
			fp_list[39]='1'
		if pure_alip==1:
			fp_list[40]='1'
#For tricyclic fused and connected
	if ringcount==3 and len(fusedpat_res)==2 and len(conpat_res)==1:
		fp_list[45]='1'
	if (len(bis2)>0) and len(bis)==0:
		if ringcount==3 and len(conpat_res)==2 and len(fusedpat_res)==0:
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[37]='1'
			if mixed_arom_two==1:
				fp_list[38]='1'
			if pure_arom==1:
				fp_list[39]='1'
			if pure_alip==1:
				fp_list[40]='1'
		if ringcount==3 and len(fusedpat_res)==3 and len(conpat_res)==0:
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
		if ringcount==3 and len(fusedpat_res)==4 and len(conpat_res)==0:
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
		if ringcount==3 and len(fusedpat_res)==5 and len(conpat_res)==0:
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
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
		#	noise.write("This is corrected frag:"+str(corrected_smiles+"\n"))
			largest=0
			fragmentcount=rc.rc(corrected_smiles)
#			print("Fragmented smiles from tr_class", corrected_smiles)
			if fragmentcount > largest:
				new_mol=Chem.MolFromSmiles(corrected_smiles)
#				if new_mol==None:
#					print("Error with the molecule")
#				else:
#				if "[al]" in fragment:
				fragment = re.sub(r'\[al.*?\]', '[Al]', fragment, re.IGNORECASE)
				new_frag=pybel.readstring("smi",fragment)
				result3=patt3.findall(new_frag)
				result4=patt4.findall(new_frag)
				result5=patt5.findall(new_frag)
				result6=patt6.findall(new_frag)
				result7=patt7.findall(new_frag)
				result8=patt8.findall(new_frag)
				res_fivear=fivear.findall(new_frag)
				res_sixar=sixar.findall(new_frag)
				conpat_res=conpat.findall(new_frag)
				fusedpat_res=fusedpat.findall(new_frag)
				if fragmentcount==3 and len(conpat_res)==2 and len(fusedpat_res)==0:
					pure_alip,pure_arom,mixed_arom_one,mixed_arom_two=ckf.checknatureforfrag(m,corrected_smiles,result3,result4,result5,result6,result7,result8,
					res_fivear,res_sixar,fusedpat_res,conpat_res)
					if mixed_arom_one==1:
						fp_list[37]='1'
					if mixed_arom_two==1:
						fp_list[38]='1'
					if pure_arom==1:
						fp_list[39]='1'
					if pure_alip==1:
						fp_list[40]='1'
				if fragmentcount==3 and len(fusedpat_res)==4 and len(conpat_res)==0:
					pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,corrected_smiles,result3,result4,result5,result6,result7,result8,res_fivear,
					res_sixar,fusedpat_res,conpat_res)
					if mixed_arom_one==1:
						fp_list[41]='1'
					if mixed_arom_two==1:
						fp_list[42]='1'
					if pure_arom==1:
						fp_list[43]='1'
					if pure_alip==1:
						fp_list[44]='1'
				if fragmentcount==3 and len(fusedpat_res)==3 and len(conpat_res)==0:
					print("Identified that the mol has 3 fusedatoms")
					pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,corrected_smiles,result3,result4,result5,result6,result7,result8,res_fivear,
					res_sixar,fusedpat_res,conpat_res)
					if mixed_arom_one==1:
						print("One ring is aromatic")
						fp_list[41]='1'
					if mixed_arom_two==1:
						fp_list[42]='1'
					if pure_arom==1:
						fp_list[43]='1'
					if pure_alip==1:
						fp_list[44]='1'
				if fragmentcount==3 and len(fusedpat_res)==5 and len(conpat_res)==0:
					pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
					if mixed_arom_one==1:
						fp_list[41]='1'
					if mixed_arom_two==1:
						fp_list[42]='1'
					if pure_arom==1:
						fp_list[43]='1'
					if pure_alip==1:
						fp_list[44]='1'
				if fragmentcount==3 and len(fusedpat_res)>=1 and len(conpat_res)>=1:
					fp_list[45]='1'
	elif len(bis2)==0:
		result3=patt3.findall(mymol)
		result4=patt4.findall(mymol)
		result5=patt5.findall(mymol)
		result6=patt6.findall(mymol)
		result7=patt7.findall(mymol)
		result8=patt8.findall(mymol)
		res_fivear=fivear.findall(mymol)
		res_sixar=sixar.findall(mymol)
		conpat_res=conpat.findall(mymol)
		fusedpat_res=fusedpat.findall(mymol)
		if ringcount==3 and len(conpat_res)==2 and len(fusedpat_res)==0:
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[37]='1'
			if mixed_arom_two==1:
				fp_list[38]='1'
			if pure_arom==1:
				fp_list[39]='1'
			if pure_alip==1:
				fp_list[40]='1'
		if ringcount==3 and len(fusedpat_res)==3 and len(conpat_res)==0: 
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
		if ringcount==3 and len(fusedpat_res)==5 and len(conpat_res)==0: 
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
		if ringcount==3 and len(fusedpat_res)==4: 
			pure_alip,pure_arom,mixed_arom_one,mixed_arom_two = ckf.checknatureforfrag(m,smiles,result3,result4,result5,result6,result7,result8,res_fivear,res_sixar,fusedpat_res,conpat_res)
			if mixed_arom_one==1:
				fp_list[41]='1'
			if mixed_arom_two==1:
				fp_list[42]='1'
			if pure_arom==1:
				fp_list[43]='1'
			if pure_alip==1:
				fp_list[44]='1'
		if ringcount==3 and len(fusedpat_res)>=1 and len(conpat_res)>=1:
			fp_list[45]='1'
#Adamantane to be classified in class 50
	if ringcount==3 and len(fusedpat_res)>=6:
			fp_list[49]='1'
	else:
		pass
	return fp_list
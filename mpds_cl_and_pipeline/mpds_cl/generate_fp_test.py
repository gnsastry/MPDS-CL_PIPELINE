"""
HJM_13-06-2023
"""
import datamol as dm

import rdkit as rdkit
from rdkit import Chem
from openbabel import pybel

from . import mono_3to19 as mono3to19
from . import bi_cl21to37 as bi21to37
from . import tr_cl38to46 as tr38to46
from . import tt_cl47to50 as tt47to50
from . import po_cl51 as po51
from . import multiple_cl20 as multiple20

patt1=pybel.Smarts("([R])")
unsat_patt=pybel.Smarts("[#6]=,#[#6]")
patt3=pybel.Smarts("[a;R]")
patt4=pybel.Smarts("[n,o,s,p;R]")
patt5=pybel.Smarts("[c]:[!c]")
patt6=pybel.Smarts("[R]=[R]")
patt7=pybel.Smarts("[R]#[R]")
patt8=pybel.Smarts("[C,S,N,O,P,A;R]")
conpat=pybel.Smarts("[R]!@[R]")
fusedpat=pybel.Smarts("[R2]")
conpat_ali=pybel.Smarts("[R&A]!@[R&A]")
fusedpat_ali=pybel.Smarts("[R2&A]")
conpat_aro=pybel.Smarts("[R&a]!@[R&a]")
fusedpat_aro=pybel.Smarts("[R2&a]")
fusedpat_mx=pybel.Smarts("[R&a][R&A]")
conpat_mx=pybel.Smarts("[R&a]!@[R&A]")
carbon_patt1=pybel.Smarts("[C,c]")
carbon_patt2=pybel.Smarts("[#6]")
ringpat=pybel.Smarts("[R]")
trans=pybel.Smarts("[#21,#22,#23,#24,#25,#26,#27,#28,#29,#30,#39,#40,#41,#42,#43,#44,#45,#46,#47,#48,#57,#58,#59,#60,#61,#62,#63,#64,#65,#66,#67,#68,#69,#70,#71,#72,#73,#74,#75,#76,#77,#78,#79,#80,#89,#90,#91,#92,#93,#94,#95,#96,#97,#98,#99,#100,#101,#102,#103,#104,#105,#106,#107,#108,#109,#110,#111,#112]")

def fp_gen(input_smiles,mol_wt):
	smiles = input_smiles

	m=Chem.MolFromSmiles(smiles)
	if m is None:
		try:
			print('M is none')
			m2=dm.to_mol(smiles,sanitize=False)
			with dm.without_rdkit_log():
				fixed_mol=dm.fix_valence_charge(m2)
				Chem.SanitizeMol(fixed_mol)
				m=Chem.MolToSmiles(fixed_mol)
		except:
			return None
	fp_final = ""
	fp_list=['0']*56
	num=[]
	mymol=pybel.readstring("smi",smiles)
	check=isinstance(m,rdkit.Chem.rdchem.Mol)
	result1=patt1.findall(mymol)
	result3=patt3.findall(mymol)
	result4=patt4.findall(mymol)
	result5=patt5.findall(mymol)
	result6=patt6.findall(mymol)
	result7=patt7.findall(mymol)
	result8=patt8.findall(mymol)
	conpat_res=conpat.findall(mymol)
	fusedpat_res=fusedpat.findall(mymol)
	carbon_res1=carbon_patt1.findall(mymol)
	carbon_res2=carbon_patt2.findall(mymol)
	ring_res=ringpat.findall(mymol)
	trans_res=trans.findall(mymol)
	number1=''
#Class 2
	if len(carbon_res1)==0:
		fp_list[1]='1'
	if len(carbon_res2)==0:
		fp_list[1]='1'
#FP for class 1 and class 2
	elif (m!=None):
		result1=patt1.findall(mymol)
		unsat_res=unsat_patt.findall(mymol)
		result3=patt3.findall(mymol)
		result4=patt4.findall(mymol)
		result5=patt5.findall(mymol)
		result6=patt6.findall(mymol)
		result7=patt7.findall(mymol)
		result8=patt8.findall(mymol)
		conpat_res=conpat.findall(mymol)
		fusedpat_res=fusedpat.findall(mymol)
		ring_res=ringpat.findall(mymol)
		inorganic1_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[C,c]"))
		inorganic2_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[#6]"))
		trans_rdk=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[#21,#22,#23,#24,#25,#26,#27,#28,#29,#30,#39,#40,#41,#42,#43,#44,#45,#46,#47,#48,#57,#58,#59,#60,#61,#62,#63,#64,#65,#66,#67,#68,#69,#70,#71,#72,#73,#74,#75,#76,#77,#78,#79,#80,#89,#90,#91,#92,#93,#94,#95,#96,#97,#98,#99,#100,#101,#102,#103,#104,#105,#106,#107,#108,#109,#110,#111,#112]"))
#Class 1
		if (len(result1)==0):
			fp_list[0]='1'
		if (len(result1)>0):
			bis = m.GetSubstructMatches(Chem.MolFromSmarts('[R]!@[!R]'))
			bis2 = m.GetSubstructMatches(Chem.MolFromSmarts('[R]!@[R]'))
#Class 3 to 19
			fp_list=mono3to19.checkmono(bis,bis2,smiles,fp_list,mymol,m,conpat,conpat_res)
#Class 20 to 36
			fp_list=bi21to37.checkbi(bis,bis2,smiles,fp_list,mymol,m)
#Class 37 to 45
			fp_list=tr38to46.checktr(bis,bis2,smiles,fp_list,mymol,m,fusedpat,fusedpat_res,conpat,conpat_res)
#Class 46 to 49
			fp_list=tt47to50.checktt(bis,bis2,smiles,fp_list,mymol,m,fusedpat,fusedpat_res,conpat,conpat_res)
#Class 50
			fp_list=po51.checkpo(bis,bis2,smiles,fp_list,m)
#Class 51
			fp_list=multiple20.checkspecial(fp_list,m)
#Class 2
		if len(carbon_res1)==0:
			fp_list[1]='1'
		if len(carbon_res2)==0:
			fp_list[1]='1'
		if len(inorganic1_rdk)==0:
			fp_list[1]='1'
		if len(inorganic2_rdk)==0:
			fp_list[1]='1'
#Class 52
		if len(trans_res)==1:
			fp_list[51]='1'
		if len(trans_rdk)==1:
			fp_list[51]='1'
#Class 53
		if len(trans_res)==2:
			fp_list[52]='1'
		if len(trans_rdk)==2:
			fp_list[52]='1'
#Class 54
		if len(trans_res)>=3:
			fp_list[53]='1'
		if len(trans_rdk)>=3:
			fp_list[53]='1'
#Class 55
		if float(mol_wt)>=750.00 and float(mol_wt)<=1200.99:
			fp_list[54]='1'
			# fp_final = "".join(fp_list)
#Class 56
		if float(mol_wt)>=1201.00:
			fp_list[55]='1'
			# fp_final = "".join(fp_list)
		fp_final = "".join(fp_list)
		
		print('<-- Fingerprint list has been generated --->')
	else:
		pass
	# print('all conditions passed')

	if len(fp_final)!=56:
		# fp_final = "".join(fp_list)
		return None
	
	result = dict(fp_final = fp_final, mol_wt = str(mol_wt), smiles = smiles)
	return result



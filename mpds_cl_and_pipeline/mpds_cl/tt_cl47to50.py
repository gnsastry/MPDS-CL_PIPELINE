import re
from openbabel import pybel
import rdkit as rdkit
from rdkit import Chem

from . import ringcount as rc
from . import cleansmiles as cs

def checktt(bis,bis2,smiles,fp_list,mymol,m,fusedpat,fusedpat_res,conpat,conpat_res):
	ringcount=rc.rc(smiles)
	mymol=pybel.readstring("smi",smiles)
	conpat=pybel.Smarts("[R]!@[R]")
	fusedpat=pybel.Smarts("[R2]")
	rdk_conpat=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[R]!@[R]"))
	rdk_fusedpat=Chem.MolFromSmiles(smiles).GetSubstructMatches(Chem.MolFromSmarts("[R2]"))
# 	if ringcount==4:
# #Class39 for all four connected rings
# 		if len(conpat_res)==3 and len(fusedpat_res)==0:
# 			fp_list[45]='1'
# #Class40
# 		if len(fusedpat_res)==4 and len(conpat_res)==0:
# 			fp_list[46]='1'
# 		if len(fusedpat_res)==5 and len(conpat_res)==0:
# 			fp_list[46]='1'
# 		if len(fusedpat_res)==6 and len(conpat_res)==0:
# 			fp_list[46]='1'
# #Class41
# 		if len(fusedpat_res)==3 and len(conpat_res)==1:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==3 and len(conpat_res)==2:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==4 and len(conpat_res)==1:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==4 and len(conpat_res)==2:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)>=5 and len(conpat_res)==1:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==2 and len(conpat_res)==2:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==2 and len(conpat_res)==1:
# 			fp_list[40]='1'
# 		if len(fusedpat_res)==1 and len(conpat_res)==2:
# 			fp_list[40]='1'						
#		if len(fusedpat_res)==3 and len(conpat_res)==1:
##			fp_list[46]='1'
#		if len(fusedpat_res)==2 and len(conpat_res)==2:
#			fp_list[46]='1'
#	#Class41 
#		if len(fusedpat_res)>6:
#			fp_list[40]='1'
##		if re.search(r'\(C1\).*?\(C3\)',smiles):
#			fp_list[40]='1'
#	if len(bis2)>0 and len(bis)==0:
#		if ringcount==4:
#		#Class38 For all four connected rings
#			if len(conpat_res)==3 and len(fusedpat_res)==0:
#				fp_list[37]='1'
#		#Class39
##			if len(fusedpat_res)==4 and len(conpat_res)==0:
#				fp_list[45]='1'
##			if len(fusedpat_res)==6 and len(conpat_res)==0:
#				fp_list[45]='1'
#		#Class40
#			if len(fusedpat_res)==4 and len(conpat_res)==1:
#				fp_list[46]='1'
#			if len(fusedpat_res)==2 and len(conpat_res)==2:
#				fp_list[46]='1'
#		#Class41
#			if len(fusedpat_res)>6:
#				fp_list[40]='1'
#			if re.search(r'\(C1\).*?\(C3\)',smiles):
#				fp_list[40]='1'
	if (len(bis)>0):
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
#			noise.write("This is fragmented frag:"+str(fragment+"\n"))
			corrected_smiles=cs.remove_non_alphabetic(fragment)
#			noise.write("This is corrected frag:"+str(corrected_smiles+"\n"))
			largest=0
			fragmentcount=rc.rc(corrected_smiles)
#			print("This is fragment count through second path of mono", fragmentcount)
			if fragmentcount > largest:
				if fragmentcount==4:
					new_mol=Chem.MolFromSmiles(corrected_smiles)
			#		if new_mol==None:
			#			print("Error with the molecule")
			#		else:
#					if "[al]" in fragment:
					fragment = re.sub(r'\[al.*?\]', '[Al]', fragment, re.IGNORECASE)
					new_frag=pybel.readstring("smi",fragment)
					conpat_res=conpat.findall(new_frag)
					fusedpat_res=fusedpat.findall(new_frag)
				#Class47 For all four connected rings
					if len(conpat_res)==3 and len(fusedpat_res)==0:
						fp_list[46]='1'
				#Class48
					if len(fusedpat_res)<=6 and len(conpat_res)==0:
						fp_list[47]='1'
				#Class49
					if len(fusedpat_res)>=5 and len(conpat_res)==1:
						fp_list[48]='1'
					if len(fusedpat_res)==4 and len(conpat_res)==1:
						fp_list[48]='1'
					if len(fusedpat_res)==4 and len(conpat_res)==2:
						fp_list[48]='1'
					if len(fusedpat_res)==3 and len(conpat_res)==1:
						fp_list[48]='1'
					if len(fusedpat_res)==2 and len(conpat_res)==2:
						fp_list[48]='1'
					if len(fusedpat_res)==3 and len(conpat_res)==2:
						fp_list[48]='1'
					if len(fusedpat_res)==1 and len(conpat_res)==2:
						fp_list[48]='1'
					if len(fusedpat_res)==2 and len(conpat_res)==1:
						fp_list[48]='1'
				#Class50
					if len(fusedpat_res)>6:
						fp_list[49]='1'
			#Class50 special for adamantane
			if fragmentcount==3 and len(fusedpat_res)>=6:
					fp_list[49]='1'
	elif len(bis2)==0:
		conpat_res=conpat.findall(mymol)
		fusedpat_res=fusedpat.findall(mymol)
		if ringcount==4:
		#Class47 For all four connected rings
			if len(conpat_res)==3 and len(fusedpat_res)==0:
				fp_list[46]='1'
		#Class48
			if len(fusedpat_res)==4 and len(conpat_res)==0:
				fp_list[47]='1'
			if len(fusedpat_res)==6 and len(conpat_res)==0:
				fp_list[47]='1'
		#Class49
			if len(fusedpat_res)==4 and len(conpat_res)==1:
				fp_list[48]='1'
			if len(fusedpat_res)==2 and len(conpat_res)==2:
				fp_list[48]='1'
			if len(fusedpat_res)>=1 and len(conpat_res)>=1:
				fp_list[48]='1'				
		#Class50
			if len(fusedpat_res)>6:
				fp_list[49]='1'
	else:
		pass
#############################################################Check for special cases using RDKIt functions######################################################################################################
# 	check_38to41="".join([e for e in fp_list[37:40]])
# #	if check_38to41.find('1')==-1:		
# #		if ringcount==4:
# #			if len(rdk_conpat)==3 and len(rdk_fusedpat)==0:
# #				fp_list[37]='1'
# #		#Class39
# #			if len(rdk_fusedpat)==4 and len(rdk_conpat)==0:
# #				fp_list[45]='1'
# #			if len(rdk_fusedpat)==6 and len(rdk_conpat)==0:
# #				fp_list[45]='1'
# #		#Class40
# #			if len(rdk_fusedpat)==4 and len(rdk_conpat)==1:
# #				fp_list[46]='1'
# #			if len(rdk_fusedpat)==2 and len(rdk_conpat)==2:
# #				fp_list[46]='1'
# ##		#Class41
# #			if len(rdk_fusedpat)>6:
# #				fp_list[40]='1'
# #			if re.search(r'\(C1\).*?\(C3\)',smiles):
# #				fp_list[40]='1'
# #		elif len(bis2)>0 and len(bis)==0:
# #			if ringcount==4:
# #			#Class38 For all four connected rings
# #				if len(rdk_conpat)==3 and len(rdk_fusedpat)==0:
# #					fp_list[37]='1'
# #			#Class39
# #				if len(rdk_fusedpat)==4 and len(rdk_conpat)==0:
# #					fp_list[45]='1'
# #				if len(rdk_fusedpat)==6 and len(rdk_conpat)==0:
# #					fp_list[45]='1'
# #			#Class40
# #				if len(rdk_fusedpat)==4 and len(rdk_conpat)==1:
# #					fp_list[46]='1'
# #				if len(rdk_fusedpat)==2 and len(rdk_conpat)==2:
# #					fp_list[46]='1'
# #			#Class41
# #				if re.search(r'\(C1\).*?\(C3\)',smiles):
# #					fp_list[40]='1'
# #				if len(rdk_fusedpat)>6:
# #					fp_list[40]='1'
# 	if (len(bis)>0):
# 			bs=[]
# 			labels=[]
# 			for bi in bis:
# 				b = m.GetBondBetweenAtoms(bi[0],bi[1])
# 				if b.GetBeginAtomIdx()==bi[0]:
# 					labels.append((10,1))
# 				else:
# 					labels.append((1,10))
# 				bs.append(b.GetIdx())
# 			nm = Chem.FragmentOnBonds(m,bs,dummyLabels=labels)
# 			frag = Chem.MolToSmiles(nm,True)
# 	#Fragmented frags
# 			fment=frag.split(".")
# 			nRings=[]
# 			for fragment in fment:
# 			#	noise.write("This is fragmented frag:"+str(fragment+"\n"))
# 				corrected_smiles=cs.remove_non_alphabetic(fragment)
# 			#	noise.write("This is corrected frag:"+str(corrected_smiles+"\n"))
# 				largest=0
# 				fragmentcount=rc.rc(corrected_smiles)
# 				print("This is fragment count through second path of mono", fragmentcount)
# 				if fragmentcount > largest:
# 					if fragmentcount==4:
# 						new_mol=Chem.MolFromSmiles(corrected_smiles)
# 						if new_mol==None:
# 							print("Error with the molecule")
# 						else:
# 							new_frag=pybel.readstring("smi",corrected_smiles)
# 							rdk_conpat=conpat.findall(new_frag)
# 							rdk_fusedpat=fusedpat.findall(new_frag)
# 						#Class38 For all four connected rings
# 							if len(rdk_conpat)==3 and len(rdk_fusedpat)==0:
# 								fp_list[37]='1'
# 						#Class39
# 							print(rdk_fusedpat)							
# 							if len(rdk_fusedpat)<=6 and len(rdk_conpat)==0:
# 								fp_list[45]='1'
# #							if len(rdk_fusedpat)==6 and len(rdk_conpat)==0:
# #								fp_list[45]='1'
# 						#Class40
# 							if len(rdk_fusedpat)==4 and len(rdk_conpat)==1:
# 								fp_list[46]='1'
# 								print("The FP value for BIT 39:",fp_list[39])
# 							if len(rdk_fusedpat)==2 and len(rdk_conpat)==2:
# 								fp_list[46]='1'
# 								print("CL40")
# 						#Class41
# 							if re.search(r'\(C1\).*?\(C3\)',smiles):
# 								fp_list[40]='1'
# 							if len(rdk_fusedpat)>6:
# 								fp_list[40]='1'
# 						#Class41 special for adamantane
# 					if fragmentcount==3 and len(rdk_fusedpat)>4:
# 							fp_list[40]='1'
	#elif len(bis2)==0:
	#		rdk_conpat=conpat.findall(mymol)
	#		rdk_fusedpat=fusedpat.findall(mymol)
	#		if ringcount==4:
	#		#Class38 For all four connected rings
	#			if len(rdk_conpat)==3 and len(rdk_fusedpat)==0:
	#				fp_list[37]='1'
	#		#Class39
	#			if len(rdk_fusedpat)==4 and len(rdk_conpat)==0:
	#				fp_list[45]='1'
	#			if len(rdk_fusedpat)==6 and len(rdk_conpat)==0:
	#				fp_list[45]='1'
	#		#Class40
	#			if len(rdk_fusedpat)==4 and len(rdk_conpat)==1:
	#				fp_list[46]='1'
	#			if len(rdk_fusedpat)==2 and len(rdk_conpat)==2:
	#				fp_list[46]='1'
	#		#Class41
	#			if len(rdk_fusedpat)>6:
	#				fp_list[40]='1'
	#			if re.search(r'\(C1\).*?\(C3\)',smiles):
	#				fp_list[40]='1'
	# else:
	# 		pass
	return fp_list
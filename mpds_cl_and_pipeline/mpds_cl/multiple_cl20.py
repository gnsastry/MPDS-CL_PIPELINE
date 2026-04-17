
import rdkit as rdkit
from rdkit.Chem import rdmolops

def checkspecial(fp_list,m):
#########################################################################################
# Find all the rings in the molecule
	ring_atoms_list = rdmolops.GetSymmSSSR(m)
	# Count the occurrences of non-carbon atoms in each ring
	atom_count = {}
	for i, ring_atoms in enumerate(ring_atoms_list):
		for atom in ring_atoms:
			symbol = m.GetAtomWithIdx(atom).GetSymbol()
			if symbol != 'C':
				if symbol == 'Li' or 'Na' or 'K' or 'Rb' or 'Cs' or 'Fr' or 'Be' or 'Mg' or 'Ca' or 'Sr' or 'Ba' or 'Ra' or 'He' or 'B' or 'N' or 'O' or 'F' or 'Ne' or 'Al' or 'Si' or 'P' or 'S' or 'Cl' or 'Ar' or 'Ga' or 'Ge' or 'As' or 'Se' or 'Br' or 'Kr' or 'In' or 'Sn' or 'Sb' or 'Te' or 'I' or 'Xe' or 'Tl' or 'Pb' or 'Bi' or 'Po' or 'At' or 'Rn' or 'Nh' or 'Fl' or 'Mc' or 'Lv' or 'Ts' or 'Og':
#			if symbol != 'C' and symbol == 'Li' or 'Na' or 'K' or 'Rb' or 'Cs' or 'Fr' or 'Be' or 'Mg' or 'Ca' or 'Sr' or 'Ba' or 'Ra' or 'He' or 'B' or 'N' or 'O' or 'F' or 'Ne' or 'Al' or 'Si' or 'P' or 'S' or 'Cl' or 'Ar' or 'Ga' or 'Ge' or 'As' or 'Se' or 'Br' or 'Kr' or 'In' or 'Sn' or 'Sb' or 'Te' or 'I' or 'Xe' or 'Tl' or 'Pb' or 'Bi' or 'Po' or 'At' or 'Rn' or 'Nh' or 'Fl' or 'Mc' or 'Lv' or 'Ts' or 'Og':
					if symbol not in atom_count:
						atom_count[symbol] = [0] * len(ring_atoms_list)
					atom_count[symbol][i] += 1
	for i, ring_atoms in enumerate(ring_atoms_list):
		total_count = sum(atom_count[symbol][i] for symbol in atom_count)
		for symbol, counts in atom_count.items():
			count = counts[i]
			if total_count > 1:
				fp_list[19]='1'
	else:
		pass
	return fp_list

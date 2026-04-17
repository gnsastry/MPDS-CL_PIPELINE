#!/usr/bin/env python
# coding: utf-8

# In[1]:
from rdkit import Chem
from rdkit.Chem import Descriptors
from . import generate_fp_test as fpg
from .mpds_id_generator import mpds_id_gen

# Read SMILES strings from a text file

def generate_mpds_cl_output(smiles):
    try:
        smiles = smiles.strip()
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            print(f"Error: Invalid SMILES string - {smiles}")
            return None

    except Exception as e:
        print(f"An error occurred for SMILES '{smiles}': {e}")
        return None
    
    mol_wt = Descriptors.MolWt(mol)
    try:
        result = fpg.fp_gen(smiles, mol_wt)
    except Exception as e:
        print(f"An error occurred for SMILES '{smiles}': {e}")
        return None
    
    result = mpds_id_gen(result)
    return result



"""
HJM_13-06-2023
"""
#!/usr/bin/python

def mpds_id_gen( result ):
	if result is None:
		return None
	
	fp = result['fp_final']
	mol_wt = result['mol_wt']
	smiles = result['smiles']
	
	mol_wt = str(mol_wt)        
	class_id = ''
	new_var = list(fp)       
	fingerprint_list = [int(x) for x in new_var]
#class 1
	if fingerprint_list[0]==1  and sum(fingerprint_list[1:55])==0:
		class_id="01"
#class 2
	if fingerprint_list[1]==1  and sum(fingerprint_list[2:55])==0:
		class_id="02"
#class 3
	if fingerprint_list[2]==1  and sum(fingerprint_list[3:55])==0:
		class_id="03"
#class 4
	if fingerprint_list[3]==1 and sum(fingerprint_list[4:55])==0:
		class_id="04"
#class 5
	if fingerprint_list[4]==1 and sum(fingerprint_list[5:55])==0:
		class_id="05"
#class 6
	if fingerprint_list[5]==1 and sum(fingerprint_list[6:55])==0:
		class_id="06"
#class 7
	if fingerprint_list[6]==1 and sum(fingerprint_list[7:55])==0:
		class_id="07"
#class 8
	if fingerprint_list[7]==1 and sum(fingerprint_list[8:55])==0:
		class_id="08"
#class 9
	if fingerprint_list[8]==1 and sum(fingerprint_list[9:55])==0:
		class_id="09"
#class 10
	if fingerprint_list[9]==1 and sum(fingerprint_list[10:55])==0:
		class_id="10"
#class 11
	if fingerprint_list[10]==1 and sum(fingerprint_list[11:55])==0:
		class_id="11"
#class 12
	if fingerprint_list[11]==1 and sum(fingerprint_list[12:55])==0:
		class_id="12"
#class 13
	if fingerprint_list[12]==1 and sum(fingerprint_list[13:55])==0:
		class_id="13"
#class 14
	if fingerprint_list[13]==1 and sum(fingerprint_list[14:55])==0:
		class_id="14"
#class 15
	if fingerprint_list[14]==1 and sum(fingerprint_list[15:55])==0:
		class_id="15"
#class 16
	if fingerprint_list[15]==1 and sum(fingerprint_list[16:55])==0:
		class_id="16"
#class 17
	if fingerprint_list[16]==1 and sum(fingerprint_list[17:55])==0:
		class_id="17"
#class 18
	if fingerprint_list[17]==1 and sum(fingerprint_list[18:55])==0:
		class_id="18"
#class 19
	if fingerprint_list[18]==1 and sum(fingerprint_list[19:55])==0:
		class_id="19"
#class 20
	if fingerprint_list[19]==1 and sum(fingerprint_list[20:55])==0:
		class_id="20"
#class 21
	if fingerprint_list[20]==1  and sum(fingerprint_list[21:55])==0:
		class_id="21"
#class 22
	if fingerprint_list[21]==1  and sum(fingerprint_list[22:55])==0:
		class_id="22"
#class 23
	if fingerprint_list[22]==1  and sum(fingerprint_list[23:55])==0:
		class_id="23"
#class 24
	if fingerprint_list[23]==1  and sum(fingerprint_list[24:55])==0:
		class_id="24"
#class 25
	if fingerprint_list[24]==1  and sum(fingerprint_list[25:55])==0:
		class_id="25"
#class 26
	if fingerprint_list[25]==1  and sum(fingerprint_list[26:55])==0:
		class_id="26"
#class 27
	if fingerprint_list[26]==1  and sum(fingerprint_list[27:55])==0:
		class_id="27"
#class 28
	if fingerprint_list[27]==1  and sum(fingerprint_list[28:55])==0:
		class_id="28"
#class 29
	if fingerprint_list[28]==1  and sum(fingerprint_list[29:55])==0:
		class_id="29"
#class 30
	if fingerprint_list[29]==1  and sum(fingerprint_list[30:55])==0:
		class_id="30"
#class 31
	if fingerprint_list[30]==1  and sum(fingerprint_list[31:55])==0:
		class_id="31"
#class 32
	if fingerprint_list[31]==1  and sum(fingerprint_list[32:55])==0:
		class_id="32"
#class 33
	if fingerprint_list[32]==1  and sum(fingerprint_list[33:55])==0:
		class_id="33"
#class 34
	if fingerprint_list[33]==1  and sum(fingerprint_list[34:55])==0:
		class_id="34"
#class 35
	if fingerprint_list[34]==1  and sum(fingerprint_list[35:55])==0:
		class_id="35"
#class 36
	if fingerprint_list[35]==1  and sum(fingerprint_list[36:55])==0:
		class_id="36"
#class 37
	if fingerprint_list[36]==1  and sum(fingerprint_list[37:55])==0:
		class_id="37"
#class 38
	if fingerprint_list[37]==1  and sum(fingerprint_list[38:55])==0:
		class_id="38"
#class 39
	if fingerprint_list[38]==1  and sum(fingerprint_list[39:55])==0:
		class_id="39"
#class 40
	if fingerprint_list[39]==1  and sum(fingerprint_list[40:55])==0:
		class_id="40"
#class 41
	if fingerprint_list[40]==1  and sum(fingerprint_list[41:55])==0:
		class_id="41"
#class 42
	if fingerprint_list[41]==1  and sum(fingerprint_list[42:55])==0:
		class_id="42"
#class 43
	if fingerprint_list[42]==1  and sum(fingerprint_list[43:55])==0:
		class_id="43"
#class 44
	if fingerprint_list[43]==1  and sum(fingerprint_list[44:55])==0:
		class_id="44"
#class 45
	if fingerprint_list[44]==1  and sum(fingerprint_list[45:55])==0:
		class_id="45"
#class 46
	if fingerprint_list[45]==1  and sum(fingerprint_list[46:55])==0:
		class_id="46"
#class 47
	if fingerprint_list[46]==1  and sum(fingerprint_list[47:55])==0:
		class_id="47"
#class 48
	if fingerprint_list[47]==1  and sum(fingerprint_list[48:55])==0:
		class_id="48"
#class 49
	if fingerprint_list[48]==1  and sum(fingerprint_list[49:55])==0:
		class_id="49"
#class 50
	if fingerprint_list[49]==1  and sum(fingerprint_list[50:55])==0:
		class_id="50"
#class 51
	if fingerprint_list[50]==1  and sum(fingerprint_list[51:55])==0:
		class_id="51"
#class 52
	if fingerprint_list[51]==1  and sum(fingerprint_list[52:55])==0:
		class_id="52"
#class 53
	if fingerprint_list[52]==1  and sum(fingerprint_list[53:55])==0:
		class_id="53"
#class 54
	if fingerprint_list[53]==1  and sum(fingerprint_list[54:55])==0:
		class_id="54"
#class 55
	if fingerprint_list[54]==1  and fingerprint_list[55]==0:
		class_id="55"
#class 56
	if fingerprint_list[55]==1:
		class_id="56"
	# output = class_id + "\t" + fp + "\t" + mol_wt + "\t" + str(smiles) + "\n"
	result['class_id'] = class_id
	#	print("<--- MPDS ID has been generated to a molecule from your dataset " + str(i) + " out of " len(lines) + "--->")
	return result


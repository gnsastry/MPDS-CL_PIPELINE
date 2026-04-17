import re
def remove_non_alphabetic(text):
    #find all occurrences of square brackets with non-alphabetic characters
	match1 = re.findall(r'\[[^a-zA-Z]*\]', text)
	match2 = re.findall(r'\([^a-zA-Z]*\)', text)
	match3 = re.findall(r'\d=',text)
#	text=re.sub(r'\[nH\]', 'n', text)
	text = re.sub(r'(\d)=(?=[^\d]|$)', r'\1', text)
	for match in match2:
		text = text.replace(match, '')# ([]) removed
	for match in match1:
		text = text.replace(match, '')# [] removed
#	for match in match3:
#		text = text.replace(match + '=', match)
	if text[0].isalpha()!=True : ## Checking the first character of the string
		if text[0]!="[":  ## Checking if the first letter is [
			text=text[1:] # if not [ then assigning text from 2nd position

	text = re.sub(r'=1(?=[^\[\]]*\])', '', text)
#	text=re.sub(r'([a-zA-Z0-9()])=([a-zA-Z0-9()])', r'\1\2', text) ## Removes = sign if it is followed by special character or alpha numeric
#	text=re.sub(r'([a-zA-Z0-9()])=', r'\1\2', text)
#	pattern = r"(?<![\[\]\(\)\w])=(?!\[|\]|\(|\)|\w)"
#	text = re.sub(pattern, "", text)
	if text[len(text)-1].isalnum()==False:
		if text[len(text)-1]!=']':
			text=text[0:len(text)-1]
	return text

#  C1=C([1*])CC=C1

# c1c2c(cc3c1Cc1ccccc1C3=)[C@@H]CCC2

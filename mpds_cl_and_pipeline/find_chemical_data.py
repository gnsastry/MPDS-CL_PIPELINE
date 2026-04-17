import re
from rdkit import Chem
from rdkit import RDLogger

# Suppress all Python warnings
# warnings.filterwarnings("ignore")

# Suppress RDKit warnings
RDLogger.DisableLog('rdApp.*')


# 4 Searches For Chemical Data
## 4.1 Drop Columns With Unequal Items

def drop_columns_with_unequal_items(df, delimiter=" "):
    """
    Drops columns from the DataFrame where values do not have an equal number of items after splitting.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        delimiter (str): Delimiter for splitting values. Default is a space.

    Returns:
        pd.DataFrame: DataFrame with the specified columns dropped.
    """
    # Columns to keep
    columns_to_keep = []

    for col in df.columns:
        # Calculate the number of items for each value after splitting
        # split_values = df[col].apply(lambda x: len(str(x).strip().split()))
        split_lengths = df[col].dropna().apply( lambda x: len(str(x).strip().split(delimiter)) )

        # Check if all rows have the same number of items
        if split_lengths.nunique() == 1:
            columns_to_keep.append(col)

    # Return the DataFrame with only the columns that meet the condition
    return df[columns_to_keep]

## 4.2 Drop Single Unique Value Columns

def drop_single_unique_value_columns(df):
    """
    Drops columns from the DataFrame that have only one unique value
    after removing null values.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with the specified columns dropped.
    """
    
    # Identify columns with only one unique value (ignoring nulls)
    columns_to_drop = [
        col for col in df.columns
        if df[col].dropna().nunique() == 1
    ]
    
    # Drop the identified columns
    df.drop(columns = columns_to_drop, inplace=True)
    return df

## 4.6 Is InChI Data
def is_InChI(x):
    inchi_pattern = re.compile(r"^InChI=\d+[A-Za-z]?/.*$")
    return bool(inchi_pattern.match(str(x)))

## 4.5 Is InChIKey Data
def is_InChIKey(x):
    inchikey_pattern = re.compile(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$")
    return bool(inchikey_pattern.match(str(x)))

## 4.3 Is SMILES Data
# Function to check if a string is a valid SMILES
def is_smiles(s):
    """Check if a string is a valid SMILES using RDKit."""
    try:
        mol = Chem.MolFromSmiles( s, sanitize = False )
        return mol is not None  # Valid if RDKit can parse it
    except Exception as e:
        print(e)
        return False
    
## 4.4 Is URL Data
def is_url(string):
    pattern = re.compile(
        r'^(https?://)?'  # Optional http or https
        r'([\da-z\.-]+)\.([a-z\.]{2,6})'  # Domain name
        r'(:\d+)?'  # Optional port
        r'(/[\w\.-]*)*'  # Optional path
        r'(\?[^\s]*)?$'  # Optional query parameters
    )
    return bool(pattern.match(string))

## 4.7 Handles Multiple ID Columns

def get_cols_with_equal_and_unequal_values(df, columns_to_check):

    # Find columns where values are equal row-wise
    same_columns = df[columns_to_check].eq(df[columns_to_check].iloc[:, 0], axis=0).all(axis=0)
    
    # Create lists of matching and non-matching columns
    matching_columns = same_columns[same_columns].index.tolist()
    non_matching_columns = same_columns[~same_columns].index.tolist()

    result = dict(
        matching_columns = matching_columns,
        non_matching_columns = non_matching_columns
    )

    # print("Columns with equal values row-wise:", matching_columns)
    # print("Columns with different values row-wise:", non_matching_columns)
    
    return result


## 4.8 Gets Found Chemical Columns  

def contains_any_chemical_column(result):
    # print('Inside contains_any_chemical_column' )
    #print('result :', result)
    
    chemical_keys = list(result.keys())
    
    chemical_keys = [ col for col in chemical_keys if col != 'ID']
    
    # print('chemical_keys :', chemical_keys)
    
    valid_cols = []
    
    for col in chemical_keys:
        valid_cols.extend(result[col])
    
    valid_cols = set(valid_cols)
    # print('valid_cols :', valid_cols)
    
    return valid_cols


## 4.9 identify_chemical_columns

def identify_chemical_columns(df):
    """
    Identifies columns in the DataFrame that contain SMILES, InChI, or InChI Key values.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        dict: Dictionary with column names categorized into 'SMILES', 'InChI', and 'InChIKey'.
    """
    # ------------------------------------------ Initialize result dictionary ------------------------------------------- #
    result = {
        "ID": [],
        "SMILES": [], 
        "InChI": [], 
        "InChIKey": [], 
        "URL": []
    }
    
    if df is None:
        return result
    # ------------------------------------------------------------------------------------------------------------------- #
    # ---------------------------------------- Drop columns with only null values --------------------------------------- #
    df.dropna(axis=1, how='all', inplace = True)
    # ------------------------------------------------------------------------------------------------------------------- #
    # -------------------- drop columns that has only one unique value after removing null values ----------------------- #
    df = drop_single_unique_value_columns(df)
    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------ drop_columns_with_unequal_items ---------------------------------------- #
    df = drop_columns_with_unequal_items(df)
    # ------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------- Define regex patterns for InChI and InChI Key --------------------------------- #
    # patterns = {
    #     "InChI": re.compile(r"^InChI=\d+[A-Za-z]?/.*$"),
    #     "InChIKey": re.compile(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$"),
    # }
    # ------------------------------------------------------------------------------------------------------------------- #

    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------ Finding SMILES InChI and InChIKey -------------------------------------- #
    # Check each column
    for col in df.columns:
        # ------------------------------------------------------------------------------------------------------------------- #
        # --------------------------------------------- Drop NaN values for processing -------------------------------------- #
        non_null_values = df[col].dropna()
        # ------------------------------------------------------------------------------------------------------------------- #
        # ------------------------------ Check for SMILES - condition - all must satisfy ------------------------------------ #
        
        # ---------------------------- Old Code ------------------------------------- #
        # # Check for SMILES
        # if non_null_values.apply( lambda x: is_smiles(str(x)) ).all():
        #     result["SMILES"].append(col)
        #     continue
        # ---------------------------- Old Code ------------------------------------- #
        
        non_null_values_unique = df[col].dropna().unique()  # Get unique non-null values
    
        # Check how many values satisfy is_smiles()
        smiles_count = sum(map(lambda x: is_smiles(str(x)), non_null_values_unique))
        
        # If the majority of unique values are SMILES, add to result
        if smiles_count > len(non_null_values_unique) / 2:
            result["SMILES"].append(col)
            continue
        # ------------------------------------------------------------------------------------------------------------------- #
        # ---------------------------- Check for InChI - condition - Any one must satisfy --------------------------------- #
        if non_null_values.apply( lambda x: is_InChI(str(x)) ).any():
        # if non_null_values.apply( lambda x: bool( patterns["InChI"].match(str(x)) )).any():
        # if non_null_values.apply(lambda x: bool(patterns["InChI"].match(str(x)))).all():
            result["InChI"].append(col)
            continue
        # --------------------------------------------------------------------------------------------------------------- #
        # --------------------------- Check for InChIKey - condition - Any one must satisfy ------------------------------ #    
        if non_null_values.apply( lambda x: is_InChIKey(str(x)) ).any():
        # if non_null_values.apply(lambda x: bool(patterns["InChIKey"].match(str(x)))).any():
        # if non_null_values.apply(lambda x: bool(patterns["InChIKey"].match(str(x)))).all():
            result["InChIKey"].append(col)
            continue
        # --------------------------------------------------------------------------------------------------------------- #
        # ----------------------------- Check for URL - condition - Any one must satisfy -------------------------------- #    
        if non_null_values.apply( lambda x: is_url(str(x)) ).any():
            result["URL"].append(col)
    # ------------------------------------------------------------------------------------------------------------------- #
    # ---------------------------------------- Dispalying result -------------------------------------------------------- #
    print('---> Search for SMILES, InChI and InChIKey completed !!!')
    print('SMILES   :', result["SMILES"])
    print('InChI    :', result["InChI"])
    print('InChIKey :', result["InChIKey"])
    print('URL      :', result["URL"])
    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------- Fetching SMILES, InChi and InChIKey ------------------------------------------ #
    
    chemical_cols = set(contains_any_chemical_column(result))
    
    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------ Select SMILES with maximum unique count -------------------------------------------- #
    
    if len( result["SMILES"] ) > 1:
        print('\n---> Search for SMILES With Maximum Unique Count !!!')
        max_unique_smiles_count = 0
        smiles = ''
    
        for smiles_col in result["SMILES"]:
            
            unique_count = df[smiles_col].nunique()
            
            if unique_count >= max_unique_smiles_count :
                max_unique_smiles_count = unique_count
                smiles = smiles_col

            print(f'\nsmiles_col : {smiles_col}')
            print(f'Count : {unique_count}')
            
            print(f'\nmax_unique_smiles_count : {max_unique_smiles_count}')
            print(f'smiles : {smiles}')
            
        result["SMILES"] = [ smiles ]
    # ------------------------------------------------------------------------------------------------------------------- #
    # --------------------------------- Selecting all remaining columns ------------------------------------------------- #
    cols = set( df.columns )
    # ------------------------------------------------------------------------------------------------------------------- #
    
    # -------------------------------------------- Finding ID column ---------------------------------------------------- #
    # ----------------------------- ID Column Values Shall or Must Be Unique and Non Null ------------------------------- #
    
    probable_db_id_cols = { col for col in df.columns if df[col].is_unique and df[col].notna().all() }
    probable_db_id_cols = probable_db_id_cols - chemical_cols
    print('\n---> Search for Database ID Completed !!!')
    print('ID       :', list( probable_db_id_cols ))
    
    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------ Handles Multiple Database ID columns ----------------------------------------- #
    
    if len( probable_db_id_cols ) > 1 :
        # Gets Columns with Equal and Unequal Values Row-wise
        multiple_id_col_details = get_cols_with_equal_and_unequal_values(df, list(probable_db_id_cols) )

        # condition to select Database ID
        if len(multiple_id_col_details['non_matching_columns']) == 0 and \
            len(multiple_id_col_details['matching_columns']) == len(probable_db_id_cols) :
            result['ID'] = [ list( probable_db_id_cols )[0] ]
    else:
        result['ID'] = list(probable_db_id_cols)
    
    # ------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------- #
    return result


def probable_id_found(result):
    if len(result['ID']) > 0:
        return True
    else:
        return False

def single_id_found(result):
    if len(result['ID']) == 1:
        return True
    else:
        return False
    

def get_canonical_smiles_inchikey(smiles):
    if smiles is None:
        return None, None
    try:
        # Convert SMILES to RDKit Molecule object
        mol = Chem.MolFromSmiles(smiles)
        if mol:
            # Generate Canonical SMILES
            canonical_smiles = Chem.MolToSmiles(mol, canonical=True)
            # Generate InChI Key
            inchi_key = Chem.MolToInchiKey(mol)
            return canonical_smiles, inchi_key
        else:
            return None, None  # Invalid SMILES
    except Exception as e:
        print(f"Error for SMILES {smiles}: {e}")
        return None, None
    
def remove_metal_hydrogens(smiles):
    """Removes explicitly mentioned hydrogen atoms from metal complexes in SMILES."""
    if smiles is None:
        return None
    else:
        pattern = r'(\[[A-Z][a-z]?(?:\d+)?)(H\d*)(\+?\d*)\]'
        cleaned_smiles = re.sub(pattern, r'\1\3]', smiles)  # Remove the H part but keep charge
        return cleaned_smiles
import os
import pandas as pd

# separators = [',', ':', '\t', '|']
def find_consistent_separator(series, separators = [',', ';', '\t', '|']):
    correct_sep = None
    for sep in separators :
        # print(f'Trying sep -> {sep}')
        split_lengths = series.dropna().apply(lambda x: len(x.split(sep)))
        item_count_after_splitting = list(set(split_lengths.to_list())) # split_lengths.unique()
        
        if len(item_count_after_splitting) == 1 and item_count_after_splitting[0] > 1:
            # print(f'Correct Separtor -> {sep}')
            correct_sep = sep
            break
    return correct_sep


def fetch_ext_sep(file):
    """Determine the file extension and separator for a given file."""
    file_extension = os.path.splitext(file)[1].lower()
    
    # Supported file types
    if file_extension not in {'.csv', '.tsv', '.txt', '.smi'}:
        print(f"Unsupported file type: {file_extension}")
        return file_extension, None

    # Common separators
    separators = [',', ';', '\t', '|']
    
    for sep in separators:
        try:
            df = pd.read_csv(file, sep=sep)
            if len(df.columns) > 1:
                return file_extension, sep
            series = df.iloc[:, 0]
            detected_sep = find_consistent_separator(series)
            if detected_sep:
                return file_extension, detected_sep
        except Exception:
            continue
    
    return file_extension, None  # No valid separator found


def create_dataframe( file, ext , sep , header=0):
    # ext = ext_sep['ext']
    # sep = ext_sep['sep']
    try:
        if ext == '.xlsx':
            df = pd.read_excel( file )
            return df
        elif any( ext in specified_file for specified_file in [ '.csv','.txt','.smi', '.tsv']):
            df = pd.read_csv( file, sep=sep, header = header)
            return df
        else:
            return pd.DataFrame() # ext_sep
    except Exception as e:
        return pd.DataFrame() # ext_sep
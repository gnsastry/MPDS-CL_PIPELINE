import os 

def centered_string(length, text):
    """
    Generates a string of a given length with the text centered in the middle,
    padded with '-' on both sides.

    Parameters:
        length (int): Total length of the resulting string.
        text (str): The text to center in the string.

    Returns:
        str: The formatted string.
    """
    # Ensure the string fits within the given length
    if len(text) > length:
        raise ValueError("Text is longer than the specified length.")

    # Calculate padding on both sides
    total_padding = length - len(text) + 2
    left_padding = total_padding // 2
    right_padding = total_padding - left_padding

    # Construct the string
    return '-' * left_padding + ' ' + text + ' ' + '-' * right_padding

def get_folder_size(folder):
    """
    Recursively calculates the size of a folder.

    Parameters:
        folder (str): The path to the folder.

    Returns:
        int: Total size of the folder in bytes.
    """
    total_size = 0
    for root, dirs, files in os.walk(folder):
        for f in files:
            file_path = os.path.join(root, f)
            total_size += os.path.getsize(file_path)
    return total_size

def get_items_sorted_by_size(directory):
    """
    Returns a sorted list of files and folders in a directory based on their size in ascending order.

    Parameters:
        directory (str): The path to the directory.

    Returns:
        list of tuples: Each tuple contains (item_name, item_size).
    """
    # List all items (files and folders) in the directory
    items = os.listdir(directory)
    
    # Create a list of tuples with item name and size
    items_with_sizes = []
    for item in items:
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path) or os.path.isdir(item_path):
            # Use os.path.getsize for files
            if os.path.isfile(item_path):
                item_size = os.path.getsize(item_path)
            # For folders, calculate the size recursively
            elif os.path.isdir(item_path):
                item_size = get_folder_size(item_path)
            items_with_sizes.append((item, item_size))
    
    # Sort the list by size
    sorted_items = sorted(items_with_sizes, key=lambda x: x[1])
    
    return sorted_items

def dir_contains_sdf(current_database_dir):
    current_database_dir_files = os.listdir(current_database_dir)
    
    for file in current_database_dir_files:
        input_file_path = os.path.join(current_database_dir, file)
        
        if file.endswith('.sdf'):
            return True
            
        elif os.path.isdir(input_file_path):
            if dir_contains_sdf(input_file_path):  # Return result of recursive call
                return True
                
    return False
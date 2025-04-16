import os

def clean_directories(base_path, log_file='non_empty_dirs.txt', action_log='log.txt', dry_run=True):
    """
    Walks through all directories under base_path.
    Deletes empty directories and logs non-empty ones.
    
    Parameters:
    - base_path (str): The path to begin the directory check.
    - log_file (str): File to log non-empty directories.
    - action_log (str): File to log all actions taken.
    - dry_run (bool): If True, simulate actions without deleting.
    """
    with open(log_file, 'w') as non_empty_log, open(action_log, 'w') as action_log_f:
        for root, dirs, files in os.walk(base_path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):
                        action_log_f.write(f"Empty: {dir_path}\n")
                        if not dry_run:
                            os.rmdir(dir_path)
                            print(f"Deleted: {dir_path}")
                        else:
                            print(f"[DRY RUN] Would delete: {dir_path}")
                    else:
                        non_empty_log.write(dir_path + '\n')
                        action_log_f.write(f"Non-empty: {dir_path}\n")
                except Exception as e:
                    error_msg = f"Error processing {dir_path}: {e}"
                    print(error_msg)
                    action_log_f.write(error_msg + '\n')

# === SAFETY FIRST ===
base_path = '../App_Details'
print(f"You're about to scan and clean: {base_path}")
proceed = input("Proceed? This may delete empty folders. Type 'YES' to continue: ")

if proceed == 'YES':
    clean_directories(base_path, dry_run=False)  # Set dry_run=False to enable deletion
else:
    print("Operation cancelled.")

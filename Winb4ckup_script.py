import config
import logging
from datetime import datetime
import time
import subprocess
import os

def list_files(directory):
    """Recursively lists all files in a given directory."""
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.relpath(os.path.join(root, file), directory))
    return file_list

def rsync_files(src_data, dest_data, log_file):
    # List files in the source and destination directories
    src_files = list_files(src_data.rstrip('/'))
    dest_files = list_files(dest_data.rstrip('/'))

    # Find files that are present in the destination but missing in the source
    deleted_files = set(dest_files) - set(src_files)

    # Log the files that have been deleted from the source
    with open(log_file, "a") as log:
        for file in deleted_files:
            log.write(f"File deleted in source but kept in destination: {file}\n")

    # Now run rsync without --delete to sync files without deleting anything in the destination
    command = [
        "rsync", 
        "-a",                # Archive mode, includes recursive copy, symbolic links, permissions, etc.
        "--itemize-changes", # Show what is being changed (transferred, skipped, etc.)
        "--out-format=%i %n",# Custom format for output, showing the action and file path
        src_data.rstrip('/') + '/',  # Ensure there is a trailing slash to copy the contents, not the directory itself
        dest_data
    ]

    # Run the rsync command and capture its output
    with open(log_file, "a") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)

def backup_user_directories(mount_point, local_path, log_file):
    user_dirs = ['Desktop', 'Documents', 'Downloads', 'Pictures', 'Music', 'Videos', 'AppData', 'Temp']
    
    # Find all user directories under 'Users/'
    users_path = os.path.join(mount_point, 'Users')
    if os.path.exists(users_path):
        for user_folder in os.listdir(users_path):
            user_folder_path = os.path.join(users_path, user_folder)
            # Exclude default/system folders
            if os.path.isdir(user_folder_path) and user_folder not in ['Default', 'Default User', 'Public', 'All Users']:
                user_backup_path = os.path.join(local_path, user_folder)
                os.makedirs(user_backup_path, exist_ok=True)  # Create destination folder for this user
                
                for user_dir in user_dirs:
                    source_dir = os.path.join(user_folder_path, user_dir)
                    dest_dir = os.path.join(user_backup_path, user_dir)  # Do not append the last folder name again
                    
                    if os.path.exists(source_dir):
                        os.makedirs(dest_dir, exist_ok=True)  # Create destination folder if it doesn't exist
                        rsync_files(source_dir, dest_dir, log_file)
                        logging.info(f"Backup of {user_dir} from {user_folder} completed using rsync.")
                    else:
                        logging.warning(f"{user_dir} does not exist in {user_folder_path}.")
    else:
        logging.warning(f"Users directory does not exist in {mount_point}.")

def main():
    # Create a timestamped log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f'{config.path}backup_{timestamp}.log'
    
    logging.basicConfig(filename=log_file, filemode='a', 
                        format='%(asctime)s | %(levelname)s | %(message)s', 
                        datefmt='%d.%m.%Y. %H:%M:%S')

    try:
        with open(config.ip_list_file, "r") as open_file:
            not_seen = [line for line in open_file]

        not_seen_now = []

        for line in not_seen:
            host_name, ip_address = line.split(',')[0], line.split(',')[1].strip()
            ping_result = subprocess.run(['ping', '-c', '1', '-W', '1', ip_address], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if ping_result.returncode == 0:
                try:
                    os.system(f'sudo mount -t cifs -o credentials={config.credentials} //{ip_address}/c$ {config.mount_point}')
                    print(f'{host_name} mounted')
                    time.sleep(2)

                    local_path = os.path.join(config.path, host_name)
                    os.makedirs(local_path, exist_ok=True)

                    if os.path.isdir(config.mount_point + 'Users/'):
                        backup_user_directories(config.mount_point, local_path, log_file)

                    os.system(f'sudo umount -a -t cifs -l')
                    logging.info(f'{host_name} backup completed and unmounted.')

                except Exception as e:
                    print(f'{host_name} not mounted')
                    logging.error(f"Mounting error for {host_name}: {str(e)}")
            else:
                print(f'{host_name} is not reachable')
                not_seen_now.append(line)

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()



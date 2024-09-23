import config
import logging
from datetime import datetime
import time
import subprocess
import os
import platform

def parse_rsync_output(line):
    """
    Parse the rsync output and convert it to human-readable log information.
    """
    change_type = line[:11].strip()
    file_path = line[12:].strip()
    if change_type.startswith('>f+++++++++'):
        return f"INFO: File '{file_path}' created and transferred."
    elif change_type.startswith('>f..t......'):
        return f"INFO: File '{file_path}' transferred and modification time updated."
    elif change_type.startswith('>f.p.......'):
        return f"INFO: File '{file_path}' transferred and permissions updated."
    elif change_type.startswith('>f..o......'):
        return f"INFO: File '{file_path}' transferred and ownership updated."
    elif change_type.startswith('>f.s.......'):
        return f"INFO: File '{file_path}' transferred and symbolic link created."
    elif change_type.startswith('>L+++++++++'):
        return f"INFO: Symbolic link '{file_path}' created."
    elif change_type.startswith('>h+++++++++'):
        return f"INFO: Hard link '{file_path}' created."
    elif change_type.startswith('>c+++++++++'):
        return f"INFO: Character device file '{file_path}' created and transferred."
    elif change_type.startswith('>b+++++++++'):
        return f"INFO: Block device file '{file_path}' created and transferred."
    elif change_type.startswith('>sf+++++++++'):
        return f"INFO: Socket file '{file_path}' created and transferred."
    elif change_type.startswith('cd+++++++++'):
        return f"INFO: Directory '{file_path}' created."
    elif change_type.startswith('.d..t......'):
        return f"INFO: Directory '{file_path}' modification time updated."
    elif change_type.startswith('.d..tp.....'):
        return f"INFO: Directory '{file_path}' modification time and permissions updated."
    elif change_type.startswith('.f'):
        return None
        # return f"INFO: File '{file_path}' skipped (no changes)."
    elif change_type.startswith('.d'):
        return None
        # return f"INFO: Directory '{file_path}' skipped (no changes)."
    elif change_type.startswith('cL'):
        return f"INFO: Symbolic link '{file_path}' updated."
    elif change_type.startswith('.L'):
        return None
        # return f"INFO: Symbolic link '{file_path}' skipped (no changes)."
    elif change_type.startswith('c'):
        return f"INFO: Character device '{file_path}' updated."
    elif change_type.startswith('b'):
        return f"INFO: Block device '{file_path}' updated."
    elif change_type.startswith('s'):
        return f"INFO: Socket file '{file_path}' updated."
    elif change_type.startswith('>'):
        return f"INFO: Special file '{file_path}' created or transferred."
    elif change_type.startswith('.d...p.....'):
        return None
        # return f"INFO: Directory '{file_path}' permissions updated."
    elif change_type.startswith('.f...p.....'):
        return None
        # return f"INFO: File '{file_path}' permissions updated."
    else:
        return f"INFO: File or directory '{file_path}' underwent some change. Rsync output: {change_type}"


def get_file_list(directory):
    """
    Get a list of all files in a given directory and its subdirectories.
    """
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list


def log_deleted_files(src_data, dest_data, log_file):
    """
    Compare the files in the source and destination directories. 
    Log the deleted files (those that are in the destination but no longer in the source).
    """
    src_files = get_file_list(src_data)
    dest_files = get_file_list(dest_data)
    src_relative_files = {os.path.relpath(file, src_data) for file in src_files}
    dest_relative_files = {os.path.relpath(file, dest_data) for file in dest_files}
    deleted_files = dest_relative_files - src_relative_files
    with open(log_file, "a") as log:
        for deleted_file in deleted_files:
            full_dest_file = os.path.join(dest_data, deleted_file)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_message = f"{timestamp} {full_dest_file}. INFO: Source file deleted. Backup copy saved.\n"
            log.write(log_message)
            logging.info(log_message.strip())


def rsync_files(src_data, dest_data, log_file):
    """
    Perform rsync and log the details, using --checksum and --partial flags.
    """
    excluded_extensions = [
        '*.exe', '*.rdp', '*.url', '*.edb', '*.css', '*.js', '*.jsm', '*.tbs', '*.nbw', '*.java', '*.php', '*.py', '*.sh',
        '*.pgp', '*.nbb', '*.msf', '*.fingerprint', '*.asc', '*.nomedia', '*.dwl2', '*.efg', '*.loaded_0', '*._paymusicid',
        '*.manifest', '*.mab', '*.mozlz4', '*.000', '*.bin', '*.cdd', '*.cue', '*.daa', '*.dao', '*.dmg', '*.img', '*.iso',
        '*.isz', '*.mdf', '*.mds', '*.mdx', '*.nrg', '*.tao', '*.tc', '*.toast', '*.uif', '*.vcd', '*.mp4', '*.mp4a',
        '*.mp3', '*.avi', '*.mkv', '*.webm', '*.mpeg', '*.wmv', '*.gz'
    ]
    exclude_params = []
    for ext in excluded_extensions:
        exclude_params.extend(['--exclude', ext])
  
    command = [
        "rsync", 
        "-a",                  # Archive mode, includes recursive copy, symbolic links, permissions, etc.
        "--itemize-changes",   # Show what is being changed (transferred, skipped, etc.)
        "--checksum",          # Use checksums to determine file differences
        "--partial",           # Keep partially transferred files to resume later
        "--out-format=%i %n"   # Custom format for output, showing the action and file path
    ] + exclude_params + [
        src_data.rstrip('/') + '/',  # Ensure there is a trailing slash to copy the contents, not the directory itself
        dest_data
    ]  
    try:
        rsync_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        with open(log_file, "a") as log:
            for line in rsync_process.stdout:
                human_readable_info = parse_rsync_output(line)
                # Skip logging if the output is None (for excluded patterns)
                if human_readable_info is None:
                    continue  # Skip writing to the log
                # Only write if human_readable_info is not None
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                full_file_path = os.path.join(dest_data, line[12:].strip())
                log_message = f"{timestamp} {full_file_path}. {human_readable_info}\n"
                log.write(log_message)
                logging.info(log_message.strip())
        rsync_process.wait()
        if rsync_process.returncode != 0:
            logging.error(f"Rsync process failed with return code {rsync_process.returncode}")
        else:
            logging.info("Rsync process completed successfully.")
    except Exception as e:
        logging.error(f"Rsync process error: {e}")
    
    log_deleted_files(src_data, dest_data, log_file)

def mount_cifs_share(ip_address):
    """
    Mount CIFS share using the provided IP address.
    """
    try:
        subprocess.run(['sudo', 'mount', '-t', 'cifs', '-o', f'credentials={config.credentials}', f'//{ip_address}/c$', config.mount_point], check=True)
        logging.info(f'{ip_address} mounted successfully')
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to mount {ip_address}: {e}")
        return False


def unmount_cifs_share():
    """
    Unmount CIFS share.
    """
    try:
        subprocess.run(['sudo', 'umount', '-a', '-t', 'cifs', '-l'], check=True)
        logging.info("Unmounted successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to unmount: {e}")


def ping_host(ip_address):
    """
    Ping host based on the current OS platform.
    """
    system = platform.system().lower()
    if system == "windows":
        return subprocess.run(['ping', '-n', '1', '-w', '1000', ip_address], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        return subprocess.run(['ping', '-c', '1', '-W', '1', ip_address], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def backup_user_directories(mount_point, local_path, log_file):
    """
    Backup user directories from the mounted CIFS share.
    """
    user_dirs = ['Desktop', 'Documents', 'Downloads', 'Pictures', 'Music', 'Videos', 'AppData', 'Favorites', 'Search']
    users_path = os.path.join(mount_point, 'Users')
    
    if os.path.exists(users_path):
        backup_users_path = os.path.join(local_path, 'Users')
        os.makedirs(backup_users_path, exist_ok=True)

        for user_folder in os.listdir(users_path):
            user_folder_path = os.path.join(users_path, user_folder)
            if os.path.isdir(user_folder_path) and user_folder not in ['Default', 'Default User', 'Public', 'All Users']:
                user_backup_path = os.path.join(backup_users_path, user_folder)
                os.makedirs(user_backup_path, exist_ok=True)

                for user_dir in user_dirs:
                    source_dir = os.path.join(user_folder_path, user_dir)
                    dest_dir = os.path.join(user_backup_path, user_dir)

                    if os.path.exists(source_dir):
                        os.makedirs(dest_dir, exist_ok=True)
                        rsync_files(source_dir, dest_dir, log_file)
                        logging.info(f"Backup of {user_dir} from {user_folder} completed using rsync.")
                    else:
                        logging.warning(f"{user_dir} does not exist in {user_folder_path}.")
    else:
        logging.warning(f"Users directory does not exist in {mount_point}.")

    # Backup Windows/Temp
    windows_dir = os.path.join(local_path, 'Windows')
    temp_dir = os.path.join(mount_point, 'Windows', 'Temp')
    if os.path.exists(temp_dir):
        temp_backup_path = os.path.join(windows_dir, 'Temp')
        os.makedirs(temp_backup_path, exist_ok=True)
        rsync_files(temp_dir, temp_backup_path, log_file)
        logging.info(f"Backup of Windows/Temp folder completed using rsync.")
    else:
        logging.warning("Windows/Temp folder does not exist.")


def main():
    """
    Main function to start the backup process.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(config.path, f'backup_{timestamp}.log')
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(filename=log_file, filemode='a', 
                        format='%(asctime)s | %(levelname)s | %(message)s', 
                        datefmt='%d.%m.%Y. %H:%M:%S')

    try:
        with open(config.ip_list_file, "r") as open_file:
            not_reachable = [line for line in open_file]

        for line in not_reachable:
            host_name, ip_address = line.split(',')[0], line.split(',')[1].strip()
            ping_result = ping_host(ip_address)
            
            if ping_result.returncode == 0:
                if mount_cifs_share(ip_address):
                    local_path = os.path.join(config.path, host_name)
                    os.makedirs(local_path, exist_ok=True)
                    if os.path.isdir(os.path.join(config.mount_point, 'Users')):
                        backup_user_directories(config.mount_point, local_path, log_file)
                    unmount_cifs_share()
                else:
                    logging.warning(f"Failed to mount {ip_address}")
            else:
                logging.warning(f'{host_name} is not reachable')

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
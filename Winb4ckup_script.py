import config
import logging
from datetime import datetime
from time import time as timeb, sleep
from time import localtime as localtimeb
from time import strftime as strftimeb
from time import gmtime as gmtimeb
import subprocess
import os
import platform
import itertools
import sys
import threading

class ConnectionMonitor(threading.Thread):
    """
    This class monitors the network connection to a specified IP address by pinging it at regular intervals.
    It runs as a separate thread to allow concurrent execution without blocking other processes.
    """
    def __init__(self, ip_address, interval=30):
        """
        Initializes the ConnectionMonitor object.
        
        Args:
        ip_address (str): The IP address to ping and monitor.
        interval (int): The interval (in seconds) between each ping request. Defaults to 30 seconds.
        """
        super().__init__() # Initialize the parent class (threading.Thread)
        self.ip_address = ip_address # IP address to monitor
        self.interval = interval # Interval for checking the connection (in seconds)
        self.is_alive = True # Connection status (True means connected)
        self.stop_monitoring = False # Flag to stop the monitoring thread

    def run(self):
        """
        This method is executed when the thread starts.
        It continuously pings the IP address at the specified interval until `stop_monitoring` is set to True.
        If the ping fails, it sets `is_alive` to False and logs a warning.
        """

        while not self.stop_monitoring:
            # Ping the IP address and check if it's reachable
            result = ping_host(self.ip_address)
            # If the ping fails (return code is non-zero), the connection is considered lost
            if result.returncode != 0:
                self.is_alive = False # Set the connection status to False
                logging.warning(f"Connection to {self.ip_address} lost.") # Log the connection loss warning
            # Wait for the specified interval before the next ping
            sleep(self.interval)

    def stop(self):
        """
        Stops the monitoring loop by setting `stop_monitoring` to True.
        This will break the while loop in the `run` method and terminate the thread.
        """
        self.stop_monitoring = True

class Spinner:
    """
    A simple spinner to show progress during long operations.
    """
    def __init__(self, message="Processing"):
        self.spinner = itertools.cycle(['|', '/', '-', '\\'])
        self.stop_running = False
        self.message = message
        self.spinner_thread = None

    def start(self):
        """
        Start the spinner in a separate thread.
        """
        self.stop_running = False
        self.spinner_thread = threading.Thread(target=self._spin)
        self.spinner_thread.start()

    def _spin(self):
        """
        Internal method to display the spinner animation.
        """
        while not self.stop_running:
            sys.stdout.write(f'\r{self.message} {next(self.spinner)} ')
            sys.stdout.flush()
            sleep(0.1)  # Use sleep from the time module

    def stop(self):
        """
        Stop the spinner and clear the line.
        """
        self.stop_running = True
        if self.spinner_thread:
            self.spinner_thread.join()
        sys.stdout.write('\r\033[K')  # Clear the line
        sys.stdout.flush()

def ping_host(ip_address):
    """
    Ping the specified IP address based on the current operating system (OS).
    This function sends a single ping request to the given IP address and returns the result.
    
    Args:
    ip_address (str): The IP address to ping.
    
    Returns:
    subprocess.CompletedProcess: The result of the ping command, including the return code and any output.
                                A return code of 0 indicates success (the host is reachable), 
                                while a non-zero return code indicates failure.
    """
    # Detect the current OS platform (e.g., Windows, Linux, macOS) and convert it to lowercase
    system = platform.system().lower()
    # If the system is Windows, use the appropriate Windows ping command format
    if system == "windows":
        # '-n 1': Send one ping request
        # '-w 1000': Timeout for each ping request is set to 1000 milliseconds (1 second)
        return subprocess.run(
            ['ping', '-n', '1', '-w', '1000', ip_address], # Ping command for Windows
            stdout=subprocess.PIPE, # Capture the standard output
            stderr=subprocess.PIPE # Capture the standard error
        )
    else:
        # For non-Windows systems (Linux, macOS), use the Unix-style ping command
        # '-c 1': Send one ping request
        # '-W 1': Wait up to 1 second for a response before timing out
        return subprocess.run(
            ['ping', '-c', '1', '-W', '1', ip_address], # Ping command for Unix-like systems
            stdout=subprocess.PIPE, # Capture the standard output
            stderr=subprocess.PIPE # Capture the standard error
        )

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
        # return None
        return f"INFO: Directory '{file_path}' permissions updated."
    elif change_type.startswith('.f...p.....'):
        # return None
        return f"INFO: File '{file_path}' permissions updated."
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

def terminate_rsync(rsync_process):
    """
    Terminate the rsync process. First, try to stop it gracefully. If it doesn't
    stop within a specified timeout, forcefully kill the process.
    """
    try:
        # Check if rsync_process is not None and is still running (poll() returns None if the process is running)
        if rsync_process and rsync_process.poll() is None:  # Check if the process is running
            # Attempt to gracefully terminate the process
            rsync_process.terminate()
            # Wait for the process to exit within 10 seconds
            rsync_process.wait(timeout=10)
            # Log that the rsync process terminated safely
            logging.info("Rsync process terminated safely.")
    except subprocess.TimeoutExpired:
        # If the process doesn't terminate within the specified time, log the error
        logging.error("Rsync did not terminate in time. Forcing kill.")
        # Forcefully kill the rsync process
        rsync_process.kill()

def rsync_files(src_data, dest_data, log_file, ip_address):
    """
    Perform rsync and monitor the connection during the transfer.
    Display a progress spinner to indicate activity.
    """
    excluded_extensions = [
        '*.exe', '*.rdp', '*.url', '*.edb', '*.css', '*.js', '*.jsm', '*.tbs', '*.nbw', '*.java', '*.php', '*.py', '*.sh',
        '*.pgp', '*.nbb', '*.msf', '*.fingerprint', '*.asc', '*.nomedia', '*.dwl2', '*.efg', '*.loaded_0', '*._paymusicid',
        '*.manifest', '*.mab', '*.mozlz4', '*.000', '*.bin', '*.cdd', '*.cue', '*.daa', '*.dao', '*.dmg', '*.img', '*.iso',
        '*.isz', '*.mdf', '*.mds', '*.mdx', '*.nrg', '*.tao', '*.tc', '*.toast', '*.uif', '*.vcd', '*.mp4', '*.mp4a',
        '*.mp3', '*.avi', '*.mkv', '*.webm', '*.mpeg', '*.wmv', '*.gz', '*.dat', '*.htm', '*.ini', '*.db', '*.db-shm', 
        '*.db-wal', '*.chk', '*.etl', '*.jfm', '*.man', '*.tbres', '*.pyd', '*.ico'
    ]
    exclude_params = []
    for ext in excluded_extensions:
        exclude_params.extend(['--exclude', ext])
  
    command = [
        "rsync", 
        "-a",                  # Archive mode
        "--itemize-changes",   # Show changes
        "--checksum",          # Use checksums
        "--partial",           # Keep partially transferred files
        "--out-format=%i %n"   # Custom format for output
    ] + exclude_params + [
        src_data.rstrip('/') + '/',  # Ensure trailing slash
        dest_data
    ]  

    # Before starting rsync, ensure the mount is valid
    if not os.path.ismount(config.mount_point):
        logging.error(f"Mount point {config.mount_point} is not valid. Rsync will not run.")
        return  # Exit if the mount point is not valid

    spinner = Spinner("Rsync in progress")
    monitor = ConnectionMonitor(ip_address)  # Start the connection monitor
    monitor.start()

    try:
        # Start the spinner to indicate the process is ongoing
        spinner.start()
        rsync_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        with open(log_file, "a") as log:
            while True:
                # Monitor rsync output
                line = rsync_process.stdout.readline()
                if not line:
                    break
                human_readable_info = parse_rsync_output(line)
                if human_readable_info:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    log_message = f"{timestamp} {human_readable_info}\n"
                    log.write(log_message)
                    logging.info(log_message.strip())

                # Check connection status from the background thread
                if not monitor.is_alive:
                    logging.warning(f"Connection to {ip_address} lost. Terminating rsync process.")
                    terminate_rsync(rsync_process)
                    unmount_cifs_share()
                    break  # Exit the loop immediately when connection is lost

        rsync_process.wait()

        # Check if rsync failed
        if rsync_process.returncode != 0:
            logging.error(f"Rsync process failed with return code {rsync_process.returncode}")
            terminate_rsync(rsync_process)  # Ensure rsync is stopped
            unmount_cifs_share()
            return  # Exit after handling rsync failure

        logging.info("Rsync process completed successfully.")
    except Exception as e:
        logging.error(f"Rsync process error: {e}")
    finally:
        # Stop the spinner and connection monitor when the process is done
        spinner.stop()
        monitor.stop()
        unmount_cifs_share()  # Ensure the share is unmounted after the process

    #log_deleted_files(src_data, dest_data, log_file)

def mount_cifs_share(ip_address):
    """
    Mount CIFS share using the provided IP address. Ensure that the mount point is valid before starting rsync.
    """
    # Unmount any previous stale mounts first
    unmount_cifs_share()

    try:
        subprocess.run(['sudo', 'mount', '-t', 'cifs', '-o', f'credentials={config.credentials}', f'//{ip_address}/c$', config.mount_point], check=True)
        logging.info(f'{ip_address} mounted successfully')

        # Verify that the mount point is accessible and valid
        if not os.path.ismount(config.mount_point):
            logging.error(f"Mount point {config.mount_point} is not valid after mounting attempt.")
            return False

        # Additional check: Ensure essential directories like 'Users' exist
        if not os.path.exists(os.path.join(config.mount_point, 'Users')):
            logging.error(f"Mount point {config.mount_point}/Users is not accessible.")
            return False

        # Additional logging to verify the directory contents
        logging.info(f"Contents of {config.mount_point}: {os.listdir(config.mount_point)}")
        
        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to mount {ip_address}: {e}")
        return False

def unmount_cifs_share():
    """
    Unmount CIFS share forcefully if needed. Use fuser to kill processes using the mount point.
    """
    try:
        # Attempt to kill any processes using the mount point, ignore error if no processes are found
        result = subprocess.run(['sudo', 'fuser', '-k', config.mount_point], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            logging.info(f"Terminated processes using {config.mount_point}")
        else:
            logging.warning(f"No processes found using {config.mount_point}. Proceeding to unmount.")

        # Attempt forceful unmount with lazy option
        subprocess.run(['sudo', 'umount', '-f', '-l', config.mount_point], check=True)
        logging.info("Unmounted successfully.")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to unmount CIFS share: {e}")


def retry_mount_cifs(ip_address, retries=3, delay=5):
    """
    Retry mounting the CIFS share a specified number of times.
    """
    for attempt in range(retries):
        if mount_cifs_share(ip_address):
            return True
        logging.warning(f"Attempt {attempt + 1} to mount {ip_address} failed. Retrying in {delay} seconds.")
        sleep(delay)
    logging.error(f"Failed to mount {ip_address} after {retries} attempts.")
    return False

def backup_user_directories(mount_point, local_path, log_file, ip_address):
    """
    Backup user directories from the mounted CIFS share.
    """
    user_dirs = ['Desktop', 'Documents', 'Downloads', 'Pictures', 'Music', 'Videos', 'AppData', 'Favorites', 'Search']
    users_path = os.path.join(mount_point, 'Users')

    # Retry mounting the share if the Users directory is not found
    def ensure_mount():
        if not os.path.exists(users_path):
            logging.warning(f"Users directory {users_path} is not accessible. Attempting to remount CIFS share.")
            if retry_mount_cifs(ip_address):
                logging.info(f"Successfully remounted CIFS share {ip_address}.")
            else:
                logging.error(f"Failed to remount CIFS share {ip_address}.")
                return False
        return True

    # Check if the Users directory exists, and retry mount if necessary
    if not ensure_mount():
        logging.error(f"Cannot proceed with backup as Users directory at {users_path} is not accessible.")
        return
    
    # Ensure the backup directory exists
    backup_users_path = os.path.join(local_path, 'Users')
    os.makedirs(backup_users_path, exist_ok=True)

    try:
        # Log the contents of the Users directory
        user_folders = os.listdir(users_path)
        logging.info(f"Found user directories: {user_folders}")
    except PermissionError as e:
        logging.error(f"Permission error accessing {users_path}: {e}")
        return
    except Exception as e:
        logging.error(f"Error accessing {users_path}: {e}")
        return

    # Iterate through each user folder
    for user_folder in user_folders:
        user_folder_path = os.path.join(users_path, user_folder)

        # Skip certain system folders
        if not os.path.isdir(user_folder_path) or user_folder in ['Default', 'Default User', 'Public', 'All Users']:
            continue

        user_backup_path = os.path.join(backup_users_path, user_folder)
        os.makedirs(user_backup_path, exist_ok=True)

        logging.info(f"Processing backup for user: {user_folder}")

        # Backup user directories like Documents, Downloads, etc.
        for user_dir in user_dirs:
            source_dir = os.path.join(user_folder_path, user_dir)
            dest_dir = os.path.join(user_backup_path, user_dir)

            # Recheck directory existence multiple times before logging it as missing
            dir_found = False
            for attempt in range(3):
                if os.path.exists(source_dir):
                    dir_found = True
                    break
                else:
                    # Try remounting the share after the first failed attempt
                    if attempt == 1 and not ensure_mount():
                        logging.error(f"Cannot access {user_dir} after remount attempt.")
                        break
                sleep(2)  # Wait for 2 seconds before retrying
            
            if dir_found:
                os.makedirs(dest_dir, exist_ok=True)
                try:
                    # Perform rsync and log success
                    rsync_files(source_dir, dest_dir, log_file, ip_address)
                    logging.info(f"Backup of {user_dir} from {user_folder} completed using rsync.")
                except Exception as e:
                    logging.error(f"Failed to backup {user_dir} from {user_folder}: {e}")
            else:
                logging.warning(f"{user_dir} does not exist in {user_folder_path} after 3 attempts. Skipping backup for this directory.")

    # Backup Windows/Temp directory
    windows_dir = os.path.join(local_path, 'Windows')
    temp_dir = os.path.join(mount_point, 'Windows', 'Temp')
    if os.path.exists(temp_dir):
        temp_backup_path = os.path.join(windows_dir, 'Temp')
        os.makedirs(temp_backup_path, exist_ok=True)
        try:
            rsync_files(temp_dir, temp_backup_path, log_file, ip_address)
            logging.info(f"Backup of Windows/Temp folder completed using rsync.")
        except Exception as e:
            logging.error(f"Failed to backup Windows/Temp: {e}")
    else:
        logging.warning("Windows/Temp folder does not exist.")

def main():
    """
    Main function to start the backup process.
    """
    start = timeb()
    start_tuple=localtimeb()
    start_time = strftimeb("%Y-%m-%d %H:%M:%S", start_tuple)
    print("Script started: "+start_time)
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
                if retry_mount_cifs(ip_address):
                    local_path = os.path.join(config.path, host_name)
                    os.makedirs(local_path, exist_ok=True)
                    if os.path.isdir(os.path.join(config.mount_point, 'Users')):
                        backup_user_directories(config.mount_point, local_path, log_file, ip_address)
                    unmount_cifs_share()
                else:
                    logging.warning(f"Failed to mount {ip_address}")
            else:
                logging.warning(f'{host_name} is not reachable')

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}", exc_info=True)
    end = timeb()
    end_tuple = localtimeb()
    end_time = strftimeb("%Y-%m-%d %H:%M:%S", end_tuple)
    print("Script ended: "+end_time)
    print("Script running time: "+strftimeb('%H:%M:%S', gmtimeb(end - start)))

if __name__ == "__main__":
    main()
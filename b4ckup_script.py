import os
from datetime import date
import shutil
import tarfile
def copyComplete(source, target):
    # copy content, stat-info (mode too), timestamps...
    shutil.copy2(source, target)
    # copy owner and group
    st = os.stat(source)
    os.chown(target, st.st_uid, st.st_gid)
file_list=[
    "/etc/hosts", \
    "/etc/hostname",\
    "/etc/rsyslog.conf",\
    "/etc/sysctl.conf",\
    "/etc/vsftpd.conf",\
    "/etc/apt/sources.list.d/localsource.list",\
    "/etc/systemd/timesyncd.conf",\
    "/etc/apt/sources.list",\
    "/etc/environment",\
    "/etc/elasticsearch/jvm.options",\
    "/etc/elasticsearch/elasticsearch.yml",\
    "/etc/kibana/kibana.yml",\
    "/etc/pam.d/common-session",\
    "/etc/pam.d/common-session-noninteractive",\
    "/etc/security/limits.conf",\
    "/etc/win-credentials",\
    "/etc/fstab",\
    "/etc/samba/smb.conf",\
    "/etc/ufw/user.rules",\
    "/etc/ufw/ufw.conf"
]
dir_list=[
    "/etc/elasticsearch/config/",\
    "/etc/kibana/config/",\
    "/etc/netplan/",\
    "/var/spool/cron/crontabs/"
]
if os.path.isfile("/etc/hostname"):
    with open("/etc/hostname", "r") as read_file:
        firstline = read_file.readline()
        hostname=firstline.rstrip()
        os.mkdir(hostname)
        for entry in file_list:
            if os.path.isfile(entry) == True:
                try:
                    os.makedirs(hostname+os.path.dirname(entry))
                except:
                    pass
                copyComplete(entry, hostname+os.path.dirname(entry)+"/"+entry.split("/")[-1])
else:
    pass
for entry in dir_list:
    if os.path.isdir(entry):
        for file in os.listdir(entry):
            if os.path.isfile(entry+file) == True:
                try: 
                    os.makedirs(hostname+os.path.dirname(entry))
                except:
                    pass
                copyComplete(entry+file, hostname+entry+file)
    else:
        pass
orginal_path=f'{hostname}_{date.today().strftime("%d%m%Y")}'
tar_gz_path= orginal_path + ".tar.gz"
with tarfile.open(tar_gz_path, "w:gz") as tar:
    tar.add(hostname)
shutil.rmtree(hostname)

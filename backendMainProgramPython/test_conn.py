import paramiko
import time

class SFTPClient:
    def __init__(self, hostname, username, password = ""):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.client = None
        self.sftp = None
        self.connect()

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.hostname, username=self.username, password=self.password)
        self.sftp = self.client.open_sftp()

    def is_connected(self):
        try:
            self.sftp.stat('.')
            return True
        except (paramiko.SSHException, EOFError):
            return False

    def reconnect(self):
        self.disconnect()
        self.connect()

    def disconnect(self):
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()

    def keep_alive(self):
        while True:
            if not self.is_connected():
                print("Connection lost. Reconnecting...")
                self.reconnect()
                print("Reconnected.")
            time.sleep(10)  # Check the connection every 10 seconds

    def upload_file(self, local_path, remote_path):
        if not self.is_connected():
            self.reconnect()
        self.sftp.put(local_path, remote_path)

    def download_file(self, remote_path, local_path):
        if not self.is_connected():
            self.reconnect()
        self.sftp.get(remote_path, local_path)

# Usage example:
sftp_client = SFTPClient("www.cloudsat.cira.colostate.edu", "d376liATuwaterloo.ca")

# Start a separate thread to keep the connection alive
import threading
keep_alive_thread = threading.Thread(target=sftp_client.keep_alive)
keep_alive_thread.daemon = True
keep_alive_thread.start()



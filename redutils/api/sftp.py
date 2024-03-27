from ..red import Red, Exit, LEVEL_ERROR

import io
import pysftp


class SFTP:
    def __init__(self, host: str, username: str, password: str, cnopts=pysftp.CnOpts()):
        """
        create a new SFTP connection

        :param host: (str) - host to connect to
        :param username: (str) - username to use
        :param password: (str) - password to use
        """
        self._host = host
        self._username = username
        self._password = password
        self._io_buffer = io.BytesIO()
        self._sftp = None
        self.cnopts = cnopts

    def get_buf(self, remote_path: str) -> io.BytesIO:
        """
        get a file from the SFTP server

        :param remote_path: (str) - remote path to the file
        """
        self._io_buffer.seek(0)
        self._io_buffer.truncate()
        self._sftp.getfo(remote_path, self._io_buffer)
        self._io_buffer.seek(0)
        return self._io_buffer

    def connect(self):
        """
        connect to the SFTP server
        """
        self._sftp = pysftp.Connection(
            host=self._host,
            username=self._username,
            password=self._password,
            cnopts=self.cnopts,
        )
        return self

    def close(self):
        """
        close the connection
        """
        self._sftp.close()
        return self

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

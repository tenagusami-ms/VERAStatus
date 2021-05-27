from datetime import date
import pathlib as p

from VERAStatus.SecZ import remote_directory, remote_file_path


def test_remote_directory():
    assert remote_directory(date(2020, 10, 26)) == p.PurePosixPath("/usr2/log/days/2020300")


def test_remote_file_path():
    assert remote_file_path(date(2020, 10, 26)) == p.PurePosixPath("/usr2/log/days/2020300/2020300.SECZ.log")

"""
Serverモジュール

スケジュール・気象データアクセスサーバ(たいていoperation)へのsshアクセスを行う。
"""
from __future__ import annotations
import dataclasses
import os
import pathlib as p
import tempfile
from contextlib import contextmanager
from typing import Tuple, Any, Mapping, Iterable, Generator

from paramiko import SSHException, AuthenticationException, SFTPAttributes, SSHClient, AutoAddPolicy

from VERAStatus.Utility import DataReadError

FileStat = SFTPAttributes
FileWithStat = Tuple[p.Path, FileStat]


@dataclasses.dataclass(frozen=True)
class ServerSettings:
    """
    サーバ設定のクラス
    """
    host: str  # サーバ
    port: int  # ポート
    user: str  # ユーザ名
    password: str  # パスワード
    schedule_directory: p.PurePath  # サーバ上のパス


def server_settings_dict2settings(settings_dict: Mapping[str, Any]) -> ServerSettings:
    """
    設定辞書を設定クラスに格納
    Args:
        settings_dict(Mapping[str, Any]: 設定辞書

    Returns:
        設定クラス(ServerSettings)
    """
    return ServerSettings(settings_dict["host"],
                          int(settings_dict["port"]),
                          settings_dict["user"],
                          settings_dict["password"],
                          p.PurePosixPath(settings_dict["schedule_path"]))


def get_command_output(server_settings: ServerSettings, command: str) -> Iterable[str]:
    """
    サーバ上でコマンドを走らせて出力を得る
    Args:
        server_settings(ServerSettings): サーバ設定
        command(Str): コマンド

    Returns:
        改行でsplitされたコマンド出力(Iterable[str])

    Raises:
        DataReadError: 接続失敗
    """
    try:
        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(hostname=server_settings.host,
                        port=server_settings.port,
                        username=server_settings.user,
                        password=server_settings.password)
            stdin, stdout, stderr = ssh.exec_command(command)
            return [f.strip() for f in stdout]
    except (SSHException, AuthenticationException, IOError) as e:
        raise DataReadError(e.args[0])


@contextmanager
def download_files(
        server_settings: ServerSettings,
        remote_directory: p.PurePath,
        local_directory=p.Path(tempfile.gettempdir()),
        path_predicate=lambda x: True
) -> Iterable[FileWithStat]:
    """
    サーバ上からファイルをダウンロードするジェネレータ
    Args:
        server_settings(ServerSettings): サーバ設定
        remote_directory(pathlib.PurePath): リモートディレクトリ
        local_directory (pathlib.Path, optional): ローカルディレクトリ。デフォルトはOSのテンポラリディレクトリ。
        path_predicate(Callable[[p.PurePath], bool], optional): ファイル名フィルタ関数。デフォルトはTrueの定数関数。

    Returns:
        ダウンロードしたファイルとファイル情報のジェネレータ(Iterable[FileWithStat])

    Raises:
        DataReadError: 接続失敗
    """
    def downloaded_file(remote_file_name: str) -> FileWithStat:
        local_file: p.Path = local_directory / remote_file_name
        sftp.get(remote_file_name, local_file)
        file_stat: SFTPAttributes = sftp.stat(remote_file_name)
        return local_file, file_stat

    downloaded_files: Iterable[FileWithStat] = list()
    try:
        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(
                hostname=server_settings.host,
                port=server_settings.port,
                username=server_settings.user,
                password=server_settings.password)
            with ssh.open_sftp() as sftp:
                sftp.chdir(str(remote_directory))
                remote_file_names: Iterable[str] = [p.PurePath(file).name for file in sftp.listdir()
                                                    if path_predicate(p.PurePath(file))]
                downloaded_files: Generator[FileWithStat, Any, None] =\
                    (downloaded_file(remote_file_name) for remote_file_name in remote_file_names)
                return downloaded_files

    except (SSHException, AuthenticationException, IOError) as e:
        raise DataReadError(e.args[0])
    finally:
        for file, _ in downloaded_files:
            if file.is_file():
                os.remove(file)

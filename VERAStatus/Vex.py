"""
Vexモジュール

vexスケジュールファイルから必要な情報を抜き出す。
"""
from __future__ import annotations

import re
from datetime import datetime
import pathlib as p
from typing import Union, Any, Optional, Match, Mapping, Iterable, MutableMapping

from .Server import ServerSettings, get_command_output
from .Utility import UTC, egrep_command
from .VERAStatus import ObservationInfo


def vex_file_keywords() -> Mapping[str, str]:
    """
    ObservationInfoクラス要素と、vexでのキーワードの対応の辞書
    Returns:
        対応辞書(Mapping[str, str])
    """
    return {
        'observation_ID': 'exper_name',
        'description': 'exper_description',
        'start_time': 'exper_nominal_start',
        'end_time': 'exper_nominal_stop',
        'PI_name': 'PI_name',
        'contact_name': 'contact_name',
        'band': 'ref $IF'
    }


def schedule_files(
        date_time: datetime,
        server_settings: ServerSettings
) -> Iterable[p.PurePath]:
    """
    サーバにある、指定された日の観測ファイルのリスト

    Args:
        date_time(datetime.datetime): 日(時刻は任意)
        server_settings(ServerSettings): サーバ設定

    Returns:
        スケジュールファイルリスト(List[pathlib.PurePath])
    """
    return [p.PurePosixPath(path) for path
            in get_command_output(
            server_settings, rf"ls {server_settings.schedule_directory}/?{date_time.strftime('%y%j')}*.vex")]


def extract_obs_info(vex_file_lines: Iterable[str]) -> Mapping[str, Any]:
    """
    vexファイルの行リストから、必要な観測情報が含まれる行の、キー・値の辞書を抜き出す。
    Args:
        vex_file_lines (Iterable[str]): vexファイルの行リスト

    Returns:
        キー・値の辞書(Mapping[str, Any])
    """
    comment_pattern = r'^\*'
    vex_lines: Iterable[str] = [line for line in vex_file_lines
                                if not re.match(comment_pattern, line) and "=" in line]
    key_values: Iterable[Iterable[str]] = [[key_value.strip().strip(";").strip() for key_value
                                            in line.strip().split("=", 1)]
                                           for line in vex_lines]
    return {key: value for key, value in key_values if key in vex_file_keywords().values()}


def vex_time2datetime(time_string: str) -> datetime:
    """
    UTC時刻文字列をdatetimeにする。
    例えば20201026012345をdatetime(2020, 10, 26, 1, 23, 45, {UTC})にする。
    Args:
        time_string(str): UTC時刻文字列

    Returns:
        datetimeオブジェクト(datetime.datetime)
    """
    return datetime.strptime(time_string + "+0000", "%Yy%jd%Hh%Mm%Ss%z")


def vex_lines2observation_info(obs_info_lines: Mapping[str, Any],
                               file_stat=None) -> ObservationInfo:
    """
    スケジュールから抜き出した観測情報辞書を、観測情報オブジェクトにする。
    Args:
        obs_info_lines(Mapping[str, Any]): 観測情報辞書
        file_stat(FileStat, optional): スケジュールファイル情報

    Returns:
        観測情報(ObservationInfo)
    """

    def convert_value(vex_key: str) -> Union[str, datetime]:
        """
        VEXスケジュールファイルの要素を格納用の形に変換
        Args:
            vex_key: VEXスケジュールファイルでのキー

        Returns:
            スケジュール要素(Union[str, datetime])
        """
        if vex_key == 'exper_nominal_start' or vex_key == 'exper_nominal_stop':
            return vex_time2datetime(obs_info_lines[vex_key])
        elif vex_key == 'ref $IF':
            matched: Optional[Match[str]] = re.search(r"^IF_([\w]+):", obs_info_lines[vex_key])
            if matched is None:
                return "unknown"
            return matched.groups()[0]
        return obs_info_lines.get(vex_key, None)

    observation_info_dict: MutableMapping[str, Any] = \
        {observation_key: convert_value(vex_key)
         for observation_key, vex_key in vex_file_keywords().items()}
    if file_stat is not None:
        observation_info_dict["timestamp"] =\
            datetime.fromtimestamp(file_stat.st_mtime, tz=UTC)
    else:
        observation_info_dict["timestamp"] = None
    correct_names(observation_info_dict)
    return ObservationInfo(**observation_info_dict)


def schedule_file2observation_info(
        server_settings: ServerSettings,
        schedule_file: p.PurePath
) -> ObservationInfo:
    """
    サーバ上のスケジュールファイルの内容を取得して観測情報にする
    Args:
        server_settings(ServerSettings): サーバ設定
        schedule_file(pathlib.PurePath): スケジュールファイルのサーバ上のパス

    Returns:
        観測情報(ObservationInfo)
    """
    lines: Iterable[str] = get_command_output(
        server_settings,
        egrep_command(schedule_file, vex_file_keywords().values()))
    obs_info_lines: Mapping[str, Any] = extract_obs_info(lines)
    return vex_lines2observation_info(obs_info_lines)


def correct_names(observation_info_dict: MutableMapping[str, Any]) -> None:
    """
    観測情報辞書にPI情報がない（＝元のスケジュールに書いてない）などの
    特殊ケースへの対応のため、観測情報辞書のPI, コンタクト情報を修正する。
    Args:
        observation_info_dict(MutableMapping[str, Any]): 観測情報辞書
    """
    if observation_info_dict["PI_name"] is None:
        if observation_info_dict["contact_name"] is None:
            observation_info_dict["contact_name"] = "unknown"
        observation_info_dict["PI_name"] = observation_info_dict["contact_name"]
    elif observation_info_dict["contact_name"] is None:
        observation_info_dict["contact_name"] = observation_info_dict["PI_name"]

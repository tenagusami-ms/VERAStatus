"""
Weatherモジュール

気象データを扱う。
"""
from __future__ import annotations

__all__ = ["Weather", "require_weather_list", "log_file_weather_server",
           "query_command_weather_server"]

from datetime import datetime, timedelta
import pathlib as p
from itertools import chain
from typing import Iterable, Mapping, Sequence

from .Server import get_command_output, ServerSettings
from .Utility import datetime2doy_string, datetime2time_string, egrep_command_remote_remote, is_empty_iterable, car_cdr

from .VERAStatus import Weather


def log_file_weather_server(date_time: datetime) -> p.PurePath:
    """
    時刻に対応する気象データサーバ(clock)上のログファイルパス
    Args:
        date_time(datetime.datetime): 時刻

    Returns:
        ログファイルパス(p.PurePath)
    """
    date_str: str = datetime2doy_string(date_time)
    return p.PurePosixPath("/usr2/log/days") / date_str / f"{date_str}.WS.log"


def query_command_weather_server(date_time_list: Iterable[datetime]) -> str:
    """
    時刻リストから、気象ログの該当行を取得するための、気象データサーバ(clock)用コマンドを生成
    Args:
        date_time_list(Iterable[datetime.datetime]): 時刻リスト

    Returns:
        コマンド(str)
    """
    time_str_list: Iterable[str] = [datetime2time_string(date_time) for date_time in date_time_list]
    return egrep_command_remote_remote(
        log_file_weather_server(next(iter(date_time_list))), time_str_list) + rf" | grep -v \;"


def uniq_lines_dict(lines_raw: Iterable[Iterable[str]]) -> Mapping[str, Iterable[str]]:
    """
    気象データには同じ時刻が書かれたデータが複数ある場合があるので、
    時刻が同じ場合はあとの時刻だけを採用する。
    Args:
        lines_raw(Iterable[Iterable[str]]): 気象データの文字列リスト

    Returns:
        uniqされた気象データ文字列リスト(Iterable[Sequence[str]])
    """
    return dict(car_cdr(line) for line in lines_raw)


def require_weather_list(
        server_settings: ServerSettings,
        date_time_list: Iterable[datetime]
) -> Sequence[Weather]:
    """
    時刻リストに対応する気象データリストをサーバから取得する。
    Args:
        server_settings: サーバ設定
        date_time_list(Iterable[datetime]): 時刻リスト

    Returns:
        気象データリスト(Sequence[Weather])
    """
    if is_empty_iterable(date_time_list):
        return list()
    lines_raw: Iterable[Iterable[str]] = \
        [line.split() for line
         in get_command_output(
            server_settings, "ssh clock -f " + query_command_weather_server(date_time_list))]
    missing_date_times: Iterable[datetime] = \
        [time for time in date_time_list
         if not time.strftime("%Y%j%H%M%S") in [next(iter(line)) for line in lines_raw]]
    if not is_empty_iterable(missing_date_times):
        lines_raw2: Iterable[Iterable[str]] = \
            [[time.strftime("%Y%j%H%M%S")] + line.split()[1:] for line, time
             in zip(
                get_command_output(
                    server_settings, "ssh clock -f " + query_command_weather_server(
                        [time + timedelta(seconds=1) for time in missing_date_times])), missing_date_times)]
        lines_raw = chain(lines_raw, lines_raw2)
    lines_dict: Mapping[str, Iterable[str]] = uniq_lines_dict(lines_raw)
    return [line2weather(date_time, list(lines_dict[date_time.strftime("%Y%j%H%M%S")]))
            for date_time in date_time_list]


def line2weather(date_time: datetime, line: Sequence[str]) -> Weather:
    """
    気象データ文字列リストを気象データにする
    Args:
        date_time(datetime.datetime): 気象データ時刻
        line(Sequence[str]): 気象データ文字列リスト

    Returns:
        気象データ(Weather)
    """
    return Weather(date_time,
                   *[float(value) for value in line[0:10]],
                   bool(line[10]),
                   *[float(value) for value in line[11:]])

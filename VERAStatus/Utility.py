"""
Utilityモジュール

各種ユーティリティ関数
"""
from __future__ import annotations

import dataclasses
import json
import math
import re
from datetime import timezone, tzinfo, timedelta, datetime
from decimal import Decimal, ROUND_HALF_UP
from functools import total_ordering
import pathlib as p
from typing import Tuple, TypeVar, Any, Union, Iterable, Generic, Mapping, Iterator, Final
import asyncio

T = TypeVar("T")

JST: tzinfo = timezone(timedelta(hours=9), "JST")  # JSTのtzinfo
UTC: tzinfo = timezone(timedelta(0), "UTC")  # UTCのtzinfo
Torr2PaCoefficient: float = 101325.0/760.0


class Error(Exception):
    """
    パッケージの例外基本クラス
    """
    pass


class DataReadError(Error):
    """
    データ読み出し失敗の例外クラス
    """
    pass


class DataWriteError(Error):
    """
    データ書き込み失敗の例外クラス
    """
    pass


class UsageError(Error):
    """
    関数の使用法が誤っているエラー
    """
    pass


class ProcessError(Error):
    """
    呼び出した実行プロセスが失敗したエラー
    """
    pass


@total_ordering
@dataclasses.dataclass(frozen=True)
class TimeData(Generic[T]):
    time: datetime  # 時刻
    value: T  # その時刻のデータ

    def __eq__(self, other):
        if not isinstance(other, TimeData):
            return NotImplemented
        return self.time == other.time

    def __lt__(self, other):
        if not isinstance(other, TimeData):
            return NotImplemented
        return self.time < other.time

    def __iter__(self):
        return (i for i in dataclasses.astuple(self))


def read_json(json_file) -> Mapping[str, Any]:
    """
    JSONファイルを読み出す
    Args:
        json_file: JSONファイル

    Returns:
        JSON辞書

    Raises:
        DataReadError: データ読み出し失敗
    """
    try:
        with open(json_file, "r") as f:
            return json.load(f)
    except OSError:
        raise DataReadError(f"data readout failed: {json_file} (module {__name__}).")


def in_jst(date_time: datetime) -> datetime:
    """
    時刻をJSTに変換
    Args:
        date_time(datetime.datetime): 時刻

    Returns:
        JST時刻(datetime.datetime)
    """
    if date_time.tzinfo is None:
        raise UsageError(f"time zone is not specified in the datetime object "
                         + f"to be converted to JST (module {__name__}).")
    return date_time.astimezone(JST)


def absolute_time_difference_second(date_time1: datetime, date_time2: datetime) -> int:
    """
    時間差の絶対値(秒単位)
    Args:
        date_time1(datetime.dateteime): 時刻1
        date_time2(datetime.dateteime): 時刻2

    Returns:
        時間差の絶対値(秒)(int)
    """
    time_difference: Final[timedelta] = date_time1 - date_time2
    return time_difference.seconds + time_difference.days * 86400


def incremented_day(day: datetime) -> datetime:
    """
    日を一日すすめる
    Args:
        day(datetime.datetime): 時刻

    Returns:
        一日後の同時刻(datetime.datetime)
    """
    return day + timedelta(days=1)


def decremented_day(day: datetime) -> datetime:
    """
    日を一日遅らせるすすめる
    Args:
        day(datetime.datetime): 時刻

    Returns:
        一日前の同時刻(datetime.datetime)
    """
    return day + timedelta(days=-1)


def round_float(r: Union[int, float], order: int) -> Union[int, float]:
    """
    実数rを、10進でorderの桁まで四捨五入。
    orderが0.1を超えたら整数を返す。
    Args:
        r(float): 実数
        order(int): 基準の桁

    Returns:
        四捨五入された実数(Union[int, float])
    """
    relative_error_tolerance: float = 1.e-15
    rounded: Decimal = Decimal(float(r) * (1.0 + relative_error_tolerance))\
        .quantize(pow(Decimal("10"), order), rounding=ROUND_HALF_UP)
    if order >= 0:
        return int(rounded)
    return float(rounded)


def doy2datetime(year: int, doy: int) -> datetime:
    """
    年と通日からUTCで00:00の時刻を持つdatetimeオブジェクトにする。
    Args:
        year(int): 年
        doy(int): 通日

    Returns:
        datetimeオブジェクト
    """
    return doy_string2datetime(str(year)+str(doy).zfill(3))


def doy_string2datetime(doy_string: str) -> datetime:
    """
        年と通日の文字列(YYYYJJJ)からUTCで00:00の時刻を持つdatetimeオブジェクトにする。
        Args:
            doy_string(str): YYYYJJJの形の、年・通日の文字列。

        Returns:
            datetimeオブジェクト

        Raises:
            UsageError: 入力文字列がYYYYJJJの形でない。
        """
    if re.match(r"\d{7}", doy_string) is None:
        raise UsageError(f"input string {doy_string} cannot be converted to datetime (module {__name__}).")
    return datetime.strptime(doy_string + '000000+0000', '%Y%j%H%M%S%z')


def time_string2datetime(time_string: str) -> datetime:
    """
    UTC時刻文字列をdatetimeにする。
    例えば20201026012345をdatetime(2020, 10, 26, 1, 23, 45, {UTC})にする。
    Args:
        time_string(str): UTC時刻文字列

    Returns:
        datetimeオブジェクト(datetime.datetime)
    """
    return datetime.strptime(time_string + "+0000", "%Y%j%H%M%S%z")


def datetime2time_string(date_time: datetime) -> str:
    """
    datetimeオブジェクトをUTC時刻文字列にする。
    例えばdatetime(2020, 10, 26, 1, 23, 45, {UTC})を"20201026012345"にする。
    Args:
        date_time(datetime.datetime): datetimeオブジェクト

    Returns:
        UTC時刻文字列(str)
    """
    return date_time.astimezone(tz=UTC).strftime('%Y%j%H%M%S')


def datetime2year_doy_string(date_time: datetime) -> Tuple[str, str]:
    """
    datetimeオブジェクトから、そのオブジェクトの時刻が入る、年と通日の文字列を返す。
    Args:
        date_time(datetime): datetimeオブジェクト

    Returns:
        年と通日の文字列(Tuple[str, str])
    """
    return date_time.strftime('%Y'), date_time.strftime('%j')


def datetime2year_doy(date_time: datetime) -> Tuple[int, int]:
    """
        datetimeオブジェクトから、そのオブジェクトの時刻が入る、年と通日を返す。
        Args:
            date_time(datetime): datetimeオブジェクト

        Returns:
            年と通日(Tuple[int, int])
        """
    return date_time.year, int(date_time.strftime('%j'))


def datetime2doy_string(date_time: datetime) -> str:
    """
    datetimeオブジェクトから、そのオブジェクトの時刻が入る、年と通日のYYYYJJJ形の文字列を返す。
    Args:
        date_time(datetime): datetimeオブジェクト

    Returns:
        年と通日の文字列(str)
    """
    year, doy = datetime2year_doy_string(date_time)
    return year + doy


def datetime2doy(date_time: datetime) -> int:
    """
    datetimeオブジェクトから、そのオブジェクトの時刻が入る、通日を返す。
    Args:
        date_time(datetime): datetimeオブジェクト

    Returns:
        通日(int)
    """
    return int(date_time.strftime('%j'))


def string_lines2string(string_lines: Iterable[str]) -> str:
    """
    文字列リストを、改行で結合された１つの文字列にする。
    Args:
        string_lines(Iterable[str]): 文字列リスト

    Returns:
        1つの文字列(str)
    """
    return "\n".join(string_lines) + "\n"


def get_now() -> datetime:
    """
    現在のUTC時刻
    Returns:
        時刻(datetime.datetime)
    """
    return datetime.now(tz=UTC)


def async_execution(tasks):
    """
    タスクの非同期実行
    Args:
        tasks(Iterable[Callable]): タスクのリスト
    """
    loop = asyncio.get_event_loop()
    future = asyncio.gather(*tasks)
    loop.run_until_complete(future)
    return future.result()


def wind_direction2octas(direction_degree: float) -> str:
    """
    風向の角度から八方位にする。
    Args:
        direction_degree(float): 風向の方位角(度)。北を0°としたCW方向。

    Returns:
        八方位の文字列(str)
    """
    if direction_degree >= 360.0 or direction_degree < 0.0:
        direction_degree = direction_degree \
            - float(math.floor(direction_degree / 360.0 + 1.e-8)) * 360.0
    direction_degree_shift = direction_degree + 360.0 / 16.0
    if direction_degree_shift < 45.0:
        return 'N'
    if direction_degree_shift < 90.0:
        return 'NE'
    if direction_degree_shift < 135.0:
        return 'E'
    if direction_degree_shift < 180.0:
        return 'SE'
    if direction_degree_shift < 225.0:
        return 'S'
    if direction_degree_shift < 270.0:
        return 'SW'
    if direction_degree_shift < 315.0:
        return 'W'
    return 'NW'


def egrep_or_str(or_string_list: Iterable[str]) -> str:
    """
    egrepコマンドでor検索をするときの"|"区切り文字列
    Args:
        or_string_list(Iterable[str]): 文字列リスト

    Returns:

    """
    return "|".join(or_string_list)


def egrep_command_remote_remote(file: p.PurePath, or_str: Union[str, Iterable[str]]) -> str:
    """
    リモートサーバからさらにリモートのサーバ上のファイルの問い合わせをするときの、
    egrepによるファイル内の文字列検索コマンドの生成
    Args:
        file(pathlib.PurePath: 対象ファイル
        or_str(Union[str, Iterable[str]]): 検索文字列、またはor検索をしたい文字列リスト

    Returns:
         検索コマンド
    """
    if type(or_str) is str:
        return f"egrep {or_str} {str(file)}"
    return rf'egrep "\"{egrep_or_str(or_str)}"\" {str(file)}'


def egrep_command(file: p.PurePath, or_str: Union[str, Iterable[str]]) -> str:
    """
    リモートサーバ上のファイルの問い合わせをするときの、
    egrepによるファイル内の文字列検索コマンドの生成
    Args:
        file(pathlib.PurePath: 対象ファイル
        or_str(Union[str, Iterable[str]]): 検索文字列、またはor検索をしたい文字列リスト

    Returns:
         検索コマンド
    """
    if type(or_str) is str:
        return f"egrep {or_str} {str(file)}"
    return rf'egrep "{egrep_or_str(or_str)}" {str(file)}'


def is_empty_iterable(iterable: Iterable[T]) -> bool:
    """
    イテラブルオブジェクトが空かどうか
    Args:
        iterable(Iterator): イテラブルオブジェクト

    Returns:
        空ならTrue
    """
    try:
        _ = next(iter(iterable))
    except StopIteration:
        return True
    return False


def car_cdr(iterable: Iterable[T]) -> Tuple[T, Iterable[T]]:
    iterator: Iterator[T] = iter(iterable)
    return next(iterator), iterator

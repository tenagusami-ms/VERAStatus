"""
SecZ module

handling secz information
"""
from __future__ import annotations

from datetime import datetime
import pathlib as p
from typing import Generator, Iterable, Sequence, Any, Tuple

from .Log import line2data
from .Server import ServerSettings, get_command_output
from .VERAStatus import SecZData
from .Weather import Weather, require_weather_list


def display_secz(secz_iter: Iterable[Tuple[SecZData, Weather]]) -> None:
    """
    secZ測定ごとのデータをディスプレイ出力
    Args:
        secz_iter(Iterable[SecZData]): 各測定ごとのデータのイテレータ
    """
    print('===========\n  sec Z\n===========')
    secz_list: Sequence[Tuple[SecZData, Weather]] = list(secz_iter)
    if len(secz_list) == 0:
        print('no sec Z data')
        return
    out_str: str = "-----------\n".join(
        [f"{str(secz_data)}{str(weather)}" for secz_data, weather in secz_list])
    print(out_str)


def remote_directory(date_time: datetime) -> p.PurePath:
    """
    リモートsecZデータディレクトリのパス
    Args:
        date_time(datetime.datetime): 日

    Returns:
        パス(pathlib.PurePosixPath)
    """
    return p.PurePosixPath("/") / "usr2" / "log" / "days" / date_time.strftime('%Y%j')


def remote_file_path(date_time: datetime) -> p.PurePath:
    """
    リモートsecZデータファイルのパス
    Args:
        date_time(datetime.datetime): ファイルが含む日時

    Returns:
        ファイルパス(pathlib.Path)
    """
    return remote_directory(date_time) / (date_time.strftime('%Y%j') + '.SECZ.log')


def secz_query_command(date_time: datetime) -> str:
    """
    指定された日時を含む日のSecZ測定結果の問い合わせコマンド
    Args:
        date_time: 日時

    Returns:
        SecZ問い合わせコマンド(str)
    """
    data_keyword: str = "TSYS1"
    date_str: str = date_time.strftime("%Y%j")
    return rf"grep {data_keyword} /usr2/log/days/{date_str}/{date_str}.SECZ.log"


def acquire_secz_data(date_time: datetime, server_settings: ServerSettings
                      ) -> Generator[SecZData, Any, None]:
    """
    指定された日時を含む日のSecZ測定結果リスト
    Args:
        date_time(datetime.datetime): 日時
        server_settings(ServerSettings): サーバ設定

    Yields:
        測定結果リスト
    """
    def command_line2secz_data(line: str) -> SecZData:
        """
        operationに送ったsecZ測定結果コマンド出力の1行からSecZDataにする
        Args:
            line: secZ測定結果コマンド出力

        Returns:
            SecZData
        """
        data_time, _, data_str_iter = line2data(line)
        data_str: Sequence[str] = list(data_str_iter)
        return SecZData(date_time=data_time,
                        optical_depth0=float(data_str[0]),
                        optical_depth1=float(data_str[1]),
                        atmospheric_temperature=float(data_str[2]),
                        receiver_temperature=float(data_str[3]),
                        system_temperature=float(data_str[4]),
                        band=data_str[5],
                        misc=data_str[6])

    return (command_line2secz_data(line)
            for line in get_command_output(server_settings, secz_query_command(date_time)))


def generate_secz(date_time: datetime, server_settings: ServerSettings
                  ) -> Iterable[Tuple[SecZData, Weather]]:
    """
    指定された日時を含む日のSecZと気象データのリスト
    Args:
        date_time(datetime.datetime): 日時
        server_settings(ServerSettings): サーバ設定

    Returns:
        (SecZ, 気象データ)タプルのリスト(Iterable[Tuple[SecZData, Weather]])
    """
    secz_list: Sequence[SecZData] = [secz for secz in acquire_secz_data(date_time, server_settings)]
    date_time_list: Sequence[datetime] = [secz.date_time for secz in secz_list]
    weather_list: Iterable[Weather] = require_weather_list(server_settings, date_time_list)
    return zip(secz_list, weather_list)

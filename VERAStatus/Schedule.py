"""
Scheduleモジュール

観測スケジュール情報の一般的な扱いを行うモジュール。
個別観測スケジュールファイルについてはVexモジュール参照。
"""
from __future__ import annotations

from concurrent.futures import Future
from concurrent.futures.process import ProcessPoolExecutor
from datetime import datetime
import pathlib as p
from threading import Thread
from typing import Iterable, Sequence, Optional

from .Server import ServerSettings
from .Utility import is_empty_iterable
from .VERAStatus import Observations, ObservationInfo
from .Vex import schedule_files, schedule_file2observation_info


class ThreadGetSchedule(Thread):
    """
    観測スケジュール取得クラス
    """
    def __init__(self, server_settings: ServerSettings, file: p.PurePath) -> None:
        super().__init__()
        self.__server_settings: ServerSettings = server_settings
        self.__file: p.PurePath = file
        self.got: Optional[ObservationInfo] = None

    def run(self) -> None:
        """
        operationのファイルを読んで観測情報を取得・格納する。
        """
        self.got = schedule_file2observation_info(self.__server_settings, self.__file)


def keywords() -> Iterable[str]:
    """
    観測スケジュールファイル内で拾う項目リスト
    Returns:
        項目リスト(Iterable[str]
    """
    return [
        'observation_ID',
        'description',
        'start_time',
        'end_time',
        'PI_name',
        'contact_name',
        'band',
        'timestamp',
    ]


def read_observations(
        date_time: datetime,
        server_settings: ServerSettings
) -> Sequence[ObservationInfo]:
    """
    指定時刻を含む日の観測情報を並行実行で取得
    Args:
        date_time(datetime.datetime): 日(時刻は任意)
        server_settings(ServerSettings): サーバ設定

    Returns:
        観測情報(Sequence[ObservationInfo])
    """
    pool_executor: ProcessPoolExecutor = ProcessPoolExecutor(max_workers=6)
    futures: Sequence[Future] = [
        pool_executor.submit(schedule_file2observation_info, server_settings, file)
        for file in schedule_files(date_time, server_settings)]
    return sorted([future.result() for future in futures])


def display_schedule(observations: Observations) -> None:
    """
    観測の表示
    Args:
        observations(Observations): 観測リスト
    """
    print('===========\n  Schedule\n===========')
    if is_empty_iterable(observations):
        print("no observations\n")
        return
    output_str: str = "-----------\n".join([observation.output_str for observation in observations])
    print(output_str)

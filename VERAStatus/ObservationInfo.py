"""
ObservationInfoモジュール

ObservationInfoクラスの基本定義
"""

import dataclasses
from datetime import datetime


@dataclasses.dataclass(frozen=True)
class ObservationInfo:
    observation_ID: str  # 観測名
    description: str  # 観測のターゲット
    start_time: datetime  # 観測開始時刻
    end_time: datetime  # 観測終了時刻
    PI_name: str  # PI名
    contact_name: str  # コンタクト先
    band: str  # 観測バンド
    timestamp: datetime  # スケジュールファイルの最終更新時刻

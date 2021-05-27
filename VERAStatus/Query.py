"""
Query モジュール

データ要求に対する提供先を振り分ける
"""
from __future__ import annotations

from datetime import datetime

from . import Schedule as Sched
from . import SecZ
from .Server import ServerSettings
from .VERAStatus import VERAStatus


def get_status_today_synchronous(today: datetime, server_settings: ServerSettings) -> VERAStatus:
    return VERAStatus(Sched.read_observations(today, server_settings),
                      SecZ.generate_secz(today, server_settings))


# def get_status_today_asynchronous(today: datetime, server_settings: ServerSettings) -> VERAStatus:
#     pass

"""
Logモジュール

観測ログを扱う。
"""
from __future__ import annotations
from datetime import datetime
from typing import Tuple, Iterable

from VERAStatus.Utility import time_string2datetime, TimeData


def extract_lines(lines: Iterable[str], key: str) -> Iterable[TimeData[str]]:
    """
    ログファイルの行から、キーが一致する行の時刻と値文字列を取り出す
    Args:
        lines(Iterable[str]): ログファイルの行リスト
        key(str): キー

    Returns:
        時刻・値文字列タプルのリスト
    """
    line_candidates: Iterable[Iterable[str]] = \
        [line.strip().split("/") for line in lines if key in line]
    return [TimeData(time=time_string2datetime(time_str.strip()), value=value.strip())
            for time_str, keyword, value in line_candidates if keyword == key]


def line2data(line: str, separator=None) -> Tuple[datetime, str, Iterable[str]]:
    """
    ログファイルの1行から、時刻・キーと、値の文字列リストを取り出す
    Args:
        line(str): ログファイルの1行
             e.g."2020280013102/TSYS1/ -0.349626  -0.742586  300.250  330.684  585.524  K  5187.000"
        separator(str, optional): 値文字列の区切り文字

    Returns:
        ログファイルのデータ(Tuple[datetime, str, List[str]])
    """
    time_str, key, data_str_list = line.strip().split("/")
    if separator is None:
        time_string2datetime(time_str.strip()), key, data_str_list.split()
    return time_string2datetime(time_str.strip()), key, data_str_list.split(separator)

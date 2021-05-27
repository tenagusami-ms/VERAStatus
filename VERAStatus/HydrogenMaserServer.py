import dataclasses
import os
import pathlib as p
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence, Final

from VERAStatus.Utility import round_float, DataReadError, JST, absolute_time_difference_second


@dataclasses.dataclass(frozen=True)
class ParameterSetting:
    """
    settings for each parameter in HM status
    """
    label: str  # name label of the parameter
    value: float  # value
    accuracy: float  # accuracy in log10
    unit: str  # unit of the value
    daily_report_index: Optional[int]  # order of report line to write into stdout


@dataclasses.dataclass(frozen=True)
class HydrogenMaserStatus:
    """
    HM status object
    """
    total_days_from_19000101: ParameterSetting
    time: ParameterSetting
    temperature_cavity: ParameterSetting
    temperature_shield1_main: ParameterSetting
    temperature_shield2_lower: ParameterSetting
    temperature_shield3_upper: ParameterSetting
    temperature_shield3_main: ParameterSetting
    temperature_shield3_lower: ParameterSetting
    temperature_electronics: ParameterSetting
    temperature_room: ParameterSetting
    H_pressure_source_kPa: ParameterSetting
    H_pressure_cell: ParameterSetting
    dissociate_intensity: ParameterSetting
    OCXO_control_voltage: ParameterSetting
    maser_RX_level: ParameterSetting
    cavity_IF_level: ParameterSetting
    cavity_automatic_tube_error_voltage: ParameterSetting
    varicap_voltage: ParameterSetting
    ion_pump_current: ParameterSetting
    ion_pump_voltage: ParameterSetting
    dissociation_drive_current: ParameterSetting
    battery_voltage: ParameterSetting
    battery_current: ParameterSetting
    battery_charge_voltage: ParameterSetting
    power_supply_voltage_plus_24: ParameterSetting
    power_supply_voltage_plus_12: ParameterSetting
    power_supply_voltage_minus_12: ParameterSetting
    power_analog_supply_voltage_plus_5: ParameterSetting
    power_digital_supply_voltage_plus_5: ParameterSetting
    power_supply_voltage_plus_3_3: ParameterSetting
    reserve: ParameterSetting

    @property
    def date_time(self) -> datetime:
        """
        time of the data set
        Returns:
            time (datetime.datetime)
        """
        return datetime.strptime(f"20{str(int(round(self.time.value * 1.e6)))}+0900", "%Y%m%d%H%M%S%z")

    @property
    def output_str(self) -> str:
        """
        string for output report
        Returns:
            string (str)
        """
        parameter_dict_to_print: Sequence[Mapping[str, Any]] = [
            parameter_setting for parameter_setting in dataclasses.asdict(self).values()
            if parameter_setting["daily_report_index"] is not None]
        return "\n".join([
            f"{parameter_setting['label']}: "
            f"{round_float(parameter_setting['value'], parameter_setting['accuracy'])}"
            f"{parameter_setting['unit']}"
            for parameter_setting
            in sorted(parameter_dict_to_print, key=lambda s: s["daily_report_index"])
        ])


@dataclasses.dataclass
class MaserSettings:
    """
    HM setting
    """
    data_prefix_directory: p.Path  # the path to the directory containing data files


def status_factory(str_value_list: Iterable[str]) -> HydrogenMaserStatus:
    """
    a line in data file to HM status
    Args:
        str_value_list(str): a list of items in the line

    Returns:
        HM Status (HydrogenMaserStatus)
    """
    float_status_list: Iterable[ParameterSetting] = [
        ParameterSetting(label=status_dict["label"],
                         value=float(str_value),
                         accuracy=status_dict["accuracy"],
                         unit=status_dict["unit"],
                         daily_report_index=status_dict.get("daily_report_index", None))
        for status_dict, str_value in zip(status_parameters(), str_value_list)]
    return HydrogenMaserStatus(*float_status_list)


def read_settings(settings_dict: Mapping[str, Any]) -> MaserSettings:
    """
    From the JSON dictionary of configuration file to HM settings
    Args:
        settings_dict(Mapping[str, Any]): the JSON dictionary

    Returns:
        Maser settings (MaserSettings)

    Raises:
        DataReadError: The specified data directory does not exist.
    """
    directory: p.Path = p.Path(settings_dict[os.name]["os_root"]) / settings_dict["data_prefix_directory"]
    if not directory.is_dir():
        raise DataReadError(f"The specified data directory {str(directory)} does not exist."
                            f"Maybe the server is down (module {__name__}).")
    return MaserSettings(data_prefix_directory=directory)


def status_parameters() -> Iterable[Mapping[str, Any]]:
    """
    settings of status parameters of HM
    Returns:
        a List of setting dictionaries for parameters (Iterable[Mapping[str, Any]])
    """
    return [
        {'label': 'total_days_from_19000101',
         'accuracy': 0, 'unit': 'days'},
        {'label': 'time', 'accuracy': 0, 'unit': ''},
        {'label': 'temperature_cavity',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_shield1_main',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_shield2_lower',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_shield3_upper',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_shield3_main',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_shield3_lower',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_electronics',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'temperature_room',
         'accuracy': -3, 'unit': 'Cdeg'},
        {'label': 'H_pressure_source_kPa',
         'accuracy': -3, 'unit': 'kPa'},
        {'label': 'H_pressure_cell',
         'accuracy': -2, 'unit': 'Pa', 'daily_report_index': 2},
        {'label': 'dissociate_intensity',
         'accuracy': 0, 'unit': '', 'daily_report_index': 1},
        {'label': 'OCXO_control_voltage',
         'accuracy': -2, 'unit': 'V', 'daily_report_index': 5},
        {'label': 'maser_RX_level',
         'accuracy': -1, 'unit': 'dBm', 'daily_report_index': 4},
        {'label': 'cavity_IF_level',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'cavity_automatic_tube_error_voltage',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'varicap_voltage',
         'accuracy': -2, 'unit': 'V', 'daily_report_index': 3},
        {'label': 'ion_pump_current',
         'accuracy': -3, 'unit': 'mA', 'daily_report_index': 0},
        {'label': 'ion_pump_voltage',
         'accuracy': -3, 'unit': 'kV'},
        {'label': 'dissociation_drive_current',
         'accuracy': -4, 'unit': 'A'},
        {'label': 'battery_voltage',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'battery_current',
         'accuracy': -4, 'unit': 'A', 'daily_report_index': 6},
        {'label': 'battery_charge_voltage',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_supply_voltage+24',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_supply_voltage+12',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_supply_voltage-12',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_analog_supply_voltage+5',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_digital_supply_voltage+5',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'power_supply_voltage+3.3',
         'accuracy': -3, 'unit': 'V'},
        {'label': 'reserve',
         'accuracy': -3, 'unit': 'Cdeg'},
    ]


def current_status(setting: MaserSettings) -> HydrogenMaserStatus:
    """
    get the current status of HM
    Args:
        setting: HM settings

    Returns:
        Status of HM (HydrogenMaserStatus)
    """
    latest_data_file: p.Path = max(*[
        p.Path(data_file) for data_file
        in setting.data_prefix_directory.glob(r"hm_only_mdata*.txt")],
              key=lambda file: os.path.getmtime(file))
    with open(latest_data_file, mode="r") as f:
        latest_line: Iterable[str] = f.readlines()[-1].strip().split("\t")

    status: HydrogenMaserStatus = status_factory(latest_line)
    latest_data_time: Final[datetime] = status.date_time
    current_time: Final[datetime] = datetime.now(tz=JST)
    if absolute_time_difference_second(latest_data_time, current_time) > 60:
        raise DataReadError(f"The server does not contain data for current time {str(current_time)}.\n"
                            f"The latest data time is {str(latest_data_time)}.\n"
                            f"Maybe the monitor software for HM is down (module {__name__}).")
    return status


def print_status(status: HydrogenMaserStatus) -> None:
    """
    Display the status to stdout
    Args:
        status(HydrogenMaserStatus): HM status
    """
    print(status.output_str)

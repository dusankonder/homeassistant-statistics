"""Main methods for the import_statistics integration."""

import os
import zoneinfo

import pandas as pd
from homeassistant.core import ServiceCall

from custom_components.import_statistics import helpers
from custom_components.import_statistics.const import (
    ATTR_DATETIME_FORMAT,
    ATTR_DECIMAL,
    ATTR_DELIMITER,
    ATTR_TIMEZONE_IDENTIFIER,
    ATTR_UNIT_FROM_ENTITY,
    DATETIME_DEFAULT_FORMAT,
    # NEW:
    ATTR_STAT_ID_CONSUMPTION,
    ATTR_STAT_ID_SUPPLY,
    ATTR_UNIT_CONSUMPTION,
    ATTR_UNIT_SUPPLY,
    CSV_COL_DATETIME,
    CSV_COL_CONSUMPTION,
    CSV_COL_SUPPLY,
)
from custom_components.import_statistics.helpers import _LOGGER, UnitFrom


def prepare_data_to_import(file_path: str, call: ServiceCall) -> tuple:
    """
    Prepare data to import statistics from a file.

    Modes:
    - B/D/F režim (preferred when supplier CSV is detected):
      Reads columns:
        B -> datetime ("Dátum a čas merania")  -> 'start'
        D -> consumption ("1.5.0 - Činný odber (kW)") -> 'consumption'
        F -> supply ("2.5.0 - Činná dodávka (kW)")    -> 'supply'
      Produces two 'sum' series: consumption & supply.

    - Fallback (original behavior):
      Reads entire CSV and expects Home Assistant-style columns, then handle_dataframe.
    """
    decimal, timezone_identifier, delimiter, datetime_format, unit_from_entity = handle_arguments(file_path, call)

    # Sane defaults if not provided
    delimiter = delimiter or ";"
    decimal = decimal or ","

    # Try B/D/F mode first (supplier CSV)
    try:
        bdf_df = pd.read_csv(
            file_path,
            sep=delimiter,
            decimal=decimal,
            quotechar='"',
            engine="python",
            usecols=[CSV_COL_DATETIME, CSV_COL_CONSUMPTION, CSV_COL_SUPPLY],
        ).rename(columns={
            CSV_COL_DATETIME: "start",
            CSV_COL_CONSUMPTION: "consumption",
            CSV_COL_SUPPLY: "supply",
        })

        stats = handle_dataframe_bdf(
            df=bdf_df,
            timezone_identifier=timezone_identifier,
            datetime_format=datetime_format,
            unit_from_where=unit_from_entity,
            stat_id_consumption=call.data.get(ATTR_STAT_ID_CONSUMPTION, "sensor.energy_consumption"),
            stat_id_supply=call.data.get(ATTR_STAT_ID_SUPPLY, "sensor.energy_supply"),
            unit_consumption=call.data.get(ATTR_UNIT_CONSUMPTION, ""),
            unit_supply=call.data.get(ATTR_UNIT_SUPPLY, ""),
        )
        return stats, unit_from_entity
    except Exception as exc:
        _LOGGER.debug("B/D/F mode not applicable (%s); falling back to original parser.", exc)

    # Fallback to original behavior (expecting statistic_id + sum/mean)
    my_df = pd.read_csv(file_path, sep=delimiter, decimal=decimal, engine="python")
    stats = handle_dataframe(my_df, timezone_identifier, datetime_format, unit_from_entity)
    return stats, unit_from_entity


def handle_arguments(file_path: str, call: ServiceCall) -> tuple:
    """
    Handle the arguments for importing statistics from a file.

    Returns:
        tuple: (decimal, timezone_identifier, delimiter, datetime_format, unit_from_entity)
    """
    decimal = "," if call.data.get(ATTR_DECIMAL, True) else "."
    datetime_format = call.data.get(ATTR_DATETIME_FORMAT, DATETIME_DEFAULT_FORMAT)
    unit_from_entity = UnitFrom.ENTITY if call.data.get(ATTR_UNIT_FROM_ENTITY) is True else UnitFrom.TABLE
    delimiter = call.data.get(ATTR_DELIMITER)

    # file existence check first
    if not os.path.exists(file_path):  # noqa: PTH110
        helpers.handle_error(f"path {file_path} does not exist.")

    timezone_identifier = call.data.get(ATTR_TIMEZONE_IDENTIFIER)
    # Validate timezone by actually constructing ZoneInfo
    try:
        _ = zoneinfo.ZoneInfo(timezone_identifier)
    except Exception:
        helpers.handle_error(f"Invalid timezone_identifier: {timezone_identifier}")

    _LOGGER.info("Importing statistics from file: %s", file_path)
    _LOGGER.debug("Timezone_identifier: %s", timezone_identifier)
    _LOGGER.debug("Delimiter: %s", delimiter)
    _LOGGER.debug("Decimal separator: %s", decimal)
    _LOGGER.debug("Datetime format: %s", datetime_format)
    _LOGGER.debug("Unit from entity: %s", unit_from_entity)

    return decimal, timezone_identifier, delimiter, datetime_format, unit_from_entity


def handle_dataframe(
    df: pd.DataFrame,
    timezone_identifier: str,
    datetime_format: str,
    unit_from_where: UnitFrom,
) -> dict:
    """
    Process a dataframe and extract statistics based on the specified columns and timezone.

    Expects columns similar to:
      - statistic_id
      - start
      - sum or mean
      - optional unit

    Raises:
      ImplementationError via helpers.handle_error if both 'mean' and 'sum' exist simultaneously
      or neither exists.
    """
    columns = df.columns
    _LOGGER.debug("Columns:")
    _LOGGER.debug(columns)

    if not helpers.are_columns_valid(df, unit_from_where):
        helpers.handle_error(
            "Implementation error. helpers.are_columns_valid returned false, this should never happen, "
            "because helpers.are_columns_valid throws an exception!"
        )

    has_mean = "mean" in columns
    has_sum = "sum" in columns

    if has_mean and has_sum:
        helpers.handle_error("Implementation error: both 'mean' and 'sum' columns are present.")
    if not has_mean and not has_sum:
        helpers.handle_error("Input must contain either a 'mean' or a 'sum' column.")

    stats = {}
    timezone = zoneinfo.ZoneInfo(timezone_identifier)

    for _index, row in df.iterrows():
        statistic_id = row["statistic_id"]
        if statistic_id not in stats:  # New statistic id found
            source = helpers.get_source(statistic_id)
            metadata = {
                "has_mean": has_mean,
                "has_sum": has_sum,
                "source": source,
                "statistic_id": statistic_id,
                "name": None,
                "unit_of_measurement": helpers.add_unit_to_dataframe(
                    source, unit_from_where, row.get("unit", ""), statistic_id
                ),
            }
            stats[statistic_id] = (metadata, [])

        if has_mean:
            new_stat = helpers.get_mean_stat(row, timezone, datetime_format)
        else:  # has_sum must be True here
            new_stat = helpers.get_sum_stat(row, timezone, datetime_format)

        stats[statistic_id][1].append(new_stat)
    return stats


def handle_dataframe_bdf(
    df: pd.DataFrame,
    timezone_identifier: str,
    datetime_format: str,
    unit_from_where: UnitFrom,
    stat_id_consumption: str,
    stat_id_supply: str,
    unit_consumption: str = "",
    unit_supply: str = "",
) -> dict:
    """
    Expects DataFrame with columns: 'start', 'consumption', 'supply'.
    Produces two 'sum' series: consumption and supply.
    """
    required = {"start", "consumption", "supply"}
    if not required.issubset(set(df.columns)):
        helpers.handle_error(f"Missing required columns: {required - set(df.columns)}")

    tz = zoneinfo.ZoneInfo(timezone_identifier)
    stats: dict[str, tuple[dict, list]] = {}

    # Create both series with metadata
    for stat_id, unit_hint in [
        (stat_id_consumption, unit_consumption),
        (stat_id_supply, unit_supply),
    ]:
        source = helpers.get_source(stat_id)
        metadata = {
            "has_mean": False,
            "has_sum": True,
            "source": source,
            "statistic_id": stat_id,
            "name": None,
            "unit_of_measurement": helpers.add_unit_to_dataframe(
                source, unit_from_where, unit_hint, stat_id
            ),
        }
        stats[stat_id] = (metadata, [])

    # Fill data as sum series
    for _, row in df.iterrows():
        # consumption -> sum
        cons_row = {"start": row["start"], "sum": row["consumption"]}
        cons_stat = helpers.get_sum_stat(cons_row, tz, datetime_format)
        stats[stat_id_consumption][1].append(cons_stat)

        # supply -> sum
        supp_row = {"start": row["start"], "sum": row["supply"]}
        supp_stat = helpers.get_sum_stat(supp_row, tz, datetime_format)
        stats[stat_id_supply][1].append(supp_stat)

    return stats

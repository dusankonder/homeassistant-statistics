"""Consts for import_statistics integration."""

DOMAIN = "import_statistics"

ATTR_FILENAME = "filename"
ATTR_TIMEZONE_IDENTIFIER = "timezone_identifier"
ATTR_DELIMITER = "delimiter"
ATTR_DECIMAL = "decimal"
ATTR_DATETIME_FORMAT = "datetime_format"
ATTR_UNIT_FROM_ENTITY = "unit_from_entity"

# --- NEW: optional service-call params for B/D/F režim ---
ATTR_STAT_ID_CONSUMPTION = "statistic_id_consumption"
ATTR_STAT_ID_SUPPLY = "statistic_id_supply"
ATTR_UNIT_CONSUMPTION = "unit_consumption"  # e.g. "kW" or "kWh"
ATTR_UNIT_SUPPLY = "unit_supply"            # e.g. "kW" or "kWh"

# --- NEW: column names as used in the supplier CSV (B/D/F) ---
CSV_COL_DATETIME = "Dátum a čas merania"        # B
CSV_COL_CONSUMPTION = "1.5.0 - Činný odber (kW)"  # D
CSV_COL_SUPPLY = "2.5.0 - Činná dodávka (kW)"     # F

TESTFILEPATHS = "tests/testfiles/"

DATETIME_DEFAULT_FORMAT = "%d.%m.%Y %H:%M"

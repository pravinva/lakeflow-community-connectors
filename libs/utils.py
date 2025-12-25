from pyspark.sql import Row
from pyspark.sql.types import *
from decimal import Decimal
from datetime import datetime
from typing import Any



def parse_value(value: Any, field_type: DataType) -> Any:
    """Convert a JSON-ish value into a PySpark-compatible value.

    NOTE: In Spark Connect / Python Data Source workers, DataType classes may come from
    different modules than the driver (so isinstance(field_type, StructType) can fail).
    We therefore dispatch using field_type.typeName() / class-name and duck-typing.
    """

    if value is None:
        return None

    try:
        type_name = field_type.typeName() if hasattr(field_type, "typeName") else None
    except Exception:
        type_name = None
    type_name = (type_name or field_type.__class__.__name__).lower()

    # -------------------- complex types --------------------
    if type_name in ("struct", "structtype") or hasattr(field_type, "fields"):
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for StructType, got {type(value)}")
        if value == {}:
            raise ValueError(
                "field in StructType cannot be an empty dict. Please assign None as the default value instead."
            )

        fields = getattr(field_type, "fields", None) or []
        field_dict = {}
        for field in fields:
            if field.name in value:
                field_dict[field.name] = parse_value(value.get(field.name), field.dataType)
            elif getattr(field, "nullable", True):
                field_dict[field.name] = None
            else:
                raise ValueError(f"Field {field.name} is not nullable but not found in the input")

        return Row(**field_dict)

    if type_name in ("array", "arraytype") or hasattr(field_type, "elementType"):
        element_type = getattr(field_type, "elementType", None)
        contains_null = getattr(field_type, "containsNull", True)
        if not isinstance(value, list):
            if contains_null:
                return [parse_value(value, element_type)]
            raise ValueError(f"Expected a list for ArrayType, got {type(value)}")
        return [parse_value(v, element_type) for v in value]

    if type_name in ("map", "maptype") or (hasattr(field_type, "keyType") and hasattr(field_type, "valueType")):
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for MapType, got {type(value)}")
        return {
            parse_value(k, field_type.keyType): parse_value(v, field_type.valueType)
            for k, v in value.items()
        }

    # -------------------- primitive types --------------------
    try:
        if type_name in ("string", "stringtype"):
            return str(value) if value is not None else None

        if type_name in ("int", "integer", "integertype", "long", "longtype", "bigint"):
            if isinstance(value, str) and value.strip():
                if "." in value:
                    return int(float(value))
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            raise ValueError(f"Cannot convert {value} to integer")

        if type_name in ("float", "floattype", "double", "doubletype"):
            if isinstance(value, str) and value.strip():
                return float(value)
            return float(value)

        if type_name in ("decimal", "decimaltype"):
            if isinstance(value, str) and value.strip():
                return Decimal(value)
            return Decimal(str(value))

        if type_name in ("boolean", "booleantype"):
            if isinstance(value, str):
                lowered = value.lower()
                if lowered in ("true", "t", "yes", "y", "1"):
                    return True
                if lowered in ("false", "f", "no", "n", "0"):
                    return False
            return bool(value)

        if type_name in ("date", "datetype"):
            if isinstance(value, str):
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                return datetime.fromisoformat(value).date()
            if isinstance(value, datetime):
                return value.date()
            raise ValueError(f"Cannot convert {value} to date")

        if type_name in ("timestamp", "timestamptype"):
            if isinstance(value, str):
                if value.endswith("Z"):
                    value = value.replace("Z", "+00:00")
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value)
            if isinstance(value, datetime):
                return value
            raise ValueError(f"Cannot convert {value} to timestamp")

        if type_name in ("null", "nulltype"):
            return None

        raise TypeError(f"Unsupported field type: {field_type}")

    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Error converting '{value}' ({type(value)}) to {field_type}: {str(e)}"
        )

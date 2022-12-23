import datetime
import json

from Serde import SERIALIZE_FIELDS


def serializer(object_serialize):
    if not isinstance(object_serialize, dict):
        raise TypeError("Serializable String not of type Dict")
    for key in list(object_serialize.keys()):
        # Add other serializer as needed
        if key in SERIALIZE_FIELDS:
            type_deserialize = SERIALIZE_FIELDS[key]
            if type_deserialize == "isoformat":
                object_serialize[key] = object_serialize[key].isoformat()
    return json.dumps(object_serialize)


def deserializer(object_deserialize):
    object_deserialize = json.loads(object_deserialize)
    for key in object_deserialize:
        if key in SERIALIZE_FIELDS:
            type_serialize = SERIALIZE_FIELDS[key]
            if type_serialize == "isoformat":
                object_deserialize[key] = datetime.datetime.fromisoformat(object_deserialize[key])
    return object_deserialize

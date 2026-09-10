"""Strict, configurable event validation; independent of Spark and Kafka."""
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

METADATA_FIELDS = ('schema_version', 'event_id', 'replay_run_id', 'replay_time',
                   'source_file', 'source_sha256', 'source_row')


def load_rules(path):
    import yaml
    rules = yaml.safe_load(Path(path).read_text())
    if not isinstance(rules, dict) or rules.get('version') != 1:
        raise ValueError('Rules must have version: 1')
    for key in ('input_fields', 'allowlist', 'required_payload', 'required_metadata',
                'supported_schema_versions'):
        values = rules.get(key)
        if not isinstance(values, list) or not all(isinstance(x, str) and x for x in values):
            raise ValueError(f'{key} must be a list of field names')
        if len(values) != len(set(values)):
            raise ValueError(f'{key} contains duplicate entries')
    fields = set(rules['input_fields'])
    if not fields or not rules['allowlist'] or not set(rules['allowlist']) <= fields:
        raise ValueError('allowlist must select declared input_fields')
    if not set(rules['required_payload']) <= fields:
        raise ValueError('required_payload must select input_fields')
    if not set(rules['required_metadata']) <= set(METADATA_FIELDS):
        raise ValueError('Unknown required metadata field')
    if rules.get('timestamp_field') not in fields or rules.get('timestamp_unit') != 'milliseconds':
        raise ValueError('A declared millisecond timestamp_field is required')
    if rules.get('invalid_timestamp') != 'preserve_null':
        raise ValueError('invalid_timestamp must be preserve_null')
    return rules


def rules_hash(rules):
    return hashlib.sha256(json.dumps(rules, sort_keys=True).encode()).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON object key')
        result[key] = value
    return result


def validate(raw, rules):
    """Return typed fields and all rejection reasons, retaining raw bytes at the sink."""
    result = dict(metadata=None, payload=None, event_time=None,
                  reasons=[], details=[], quality_flags=[])
    def reject(code, detail):
        if code not in result['reasons']:
            result['reasons'].append(code)
        result['details'].append(detail)
    try:
        event = json.loads(bytes(raw).decode('utf-8'), object_pairs_hook=unique_object,
                           parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, UnicodeError, TypeError):
        reject('MALFORMED_JSON', 'value must be UTF-8 JSON with unique object keys')
        return result
    if not isinstance(event, dict):
        reject('INVALID_DATA_TYPE', 'event must be an object')
        return result
    meta, payload = event.get('metadata'), event.get('payload')
    for name, value in (('metadata', meta), ('payload', payload)):
        if value is None:
            reject('MISSING_REQUIRED_FIELD', name)
        elif not isinstance(value, dict):
            reject('INVALID_DATA_TYPE', name)
    if not isinstance(meta, dict) or not isinstance(payload, dict):
        return result
    for section, values, required in (('metadata', meta, rules['required_metadata']),
                                      ('payload', payload, rules['required_payload'])):
        for field in required:
            if values.get(field) is None or values.get(field) == '':
                reject('MISSING_REQUIRED_FIELD', f'{section}.{field}')
    typed_meta = {}
    for field in METADATA_FIELDS:
        value = meta.get(field)
        valid = value is None or (type(value) is int and 0 < value <= 9223372036854775807 if field == 'source_row' else isinstance(value, str))
        if not valid:
            reject('INVALID_DATA_TYPE', f'metadata.{field}')
        typed_meta[field] = value if valid else None
    result['metadata'] = typed_meta
    if meta.get('schema_version') not in rules['supported_schema_versions']:
        reject('UNSUPPORTED_SCHEMA_VERSION', 'metadata.schema_version')
    typed_payload = {}
    for field in rules['input_fields']:
        value = payload.get(field)
        if value is not None and not isinstance(value, str):
            if field != rules['timestamp_field']:
                reject('INVALID_DATA_TYPE', f'payload.{field}')
            value = None
        typed_payload[field] = value
    result['payload'] = typed_payload
    field = rules['timestamp_field']
    timestamp = payload.get(field)
    if timestamp is None or timestamp == '':
        result['quality_flags'].append('MISSING_EVENT_TIMESTAMP')
        typed_payload[field] = None
    elif not isinstance(timestamp, str) or not re.fullmatch(r'-?[0-9]+', timestamp):
        result['quality_flags'].append('INVALID_EVENT_TIMESTAMP')
        typed_payload[field] = None
    else:
        try:
            result['event_time'] = (datetime(1970, 1, 1, tzinfo=timezone.utc)
                                    + timedelta(milliseconds=int(timestamp)))
        except (ValueError, OverflowError):
            result['quality_flags'].append('INVALID_EVENT_TIMESTAMP')
            typed_payload[field] = None
    return result

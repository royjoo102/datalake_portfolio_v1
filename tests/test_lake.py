import hashlib
import json
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from lake.schema import partition_date
from lake.writer import Lake
from streaming.rules import load_rules, validate
from tests.test_streaming import sample, encoded, RULES


class MemoryObjects:
    def __init__(self):
        self.data={}
    def json(self,key):
        return json.loads(self.data[key]) if key in self.data else None
    def put(self,key,data,immutable=False):
        if immutable and key in self.data and self.data[key]!=data:
            raise ValueError('immutable conflict')
        self.data[key]=data
        return dict(key=key,bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
    def put_json(self,key,value,immutable=False):
        return self.put(key,json.dumps(value,sort_keys=True).encode(),immutable)
    def list(self,prefix):
        return sorted(k for k in self.data if k.startswith(prefix))


class LakeTests(unittest.TestCase):
    def setUp(self):
        self.rules=load_rules(RULES)
        self.objects=MemoryObjects()
        self.lake=Lake(self.objects,'test',self.rules)
    def row(self,offset=0,raw=None):
        raw=encoded(sample()) if raw is None else raw
        return dict(validate(raw,self.rules),topic='test',partition=0,offset=offset,value=raw,timestamp=None)
    def test_los_angeles_midnight_and_dst(self):
        self.assertEqual(partition_date('2026-06-07T04:59:45Z'),'2026-06-06')
        self.assertEqual(partition_date('2026-01-01T07:59:59Z'),'2025-12-31')
        self.assertEqual(partition_date('2026-01-01T08:00:00Z'),'2026-01-01')
        self.assertEqual(partition_date('2026-03-08T10:00:00Z'),'2026-03-08')
        self.assertEqual(partition_date(None),'unknown')
    def test_partial_upload_invisible_and_retry_idempotent(self):
        rows=[self.row(0),self.row(1,b'{bad')]
        with self.assertRaises(RuntimeError):
            self.lake.publish('batch',rows,fail_after_zone='raw')
        self.assertEqual(self.objects.list('_control/test/commits/'),[])
        self.assertTrue(self.objects.list('raw/'))
        first=self.lake.publish('batch',rows)
        keys=list(self.objects.data)
        second=self.lake.publish('batch',rows)
        self.assertEqual(list(self.objects.data),keys)
        self.assertTrue(second['retried'])
        self.assertEqual(first['counts'],dict(raw=2,silver=1,rejected=1))
    def test_changed_batch_is_refused(self):
        self.lake.publish('batch',[self.row(0)])
        with self.assertRaises(ValueError):
            self.lake.publish('batch',[self.row(1)])
    def test_business_duplicates_and_raw_bytes_preserved(self):
        manifest=self.lake.publish('batch',[self.row(0),self.row(1)])
        self.assertEqual(manifest['counts']['silver'],2)
        for f in manifest['files']:
            table=pq.read_table(pa.BufferReader(self.objects.data[f['key']]))
            if f['zone']=='raw':
                self.assertEqual(table['raw_value'].to_pylist(),[encoded(sample())]*2)
            if f['zone']=='silver':
                self.assertNotIn('payload',table.column_names)
                self.assertNotIn('client_client_id',table.column_names)
                self.assertTrue(set(self.rules['allowlist']).issubset(table.column_names))
    def test_null_timestamp_kept_in_silver(self):
        event=sample();event['payload']['event_timestamp']='broken'
        manifest=self.lake.publish('batch',[self.row(0,encoded(event))])
        f=next(f for f in manifest['files'] if f['zone']=='silver')
        row=pq.read_table(pa.BufferReader(self.objects.data[f['key']])).to_pylist()[0]
        self.assertEqual(row['event_date'],'unknown')
        self.assertIsNone(row['event_timestamp'])
        self.assertEqual(row['quality_flags'],['INVALID_EVENT_TIMESTAMP'])
    def test_incomplete_import_cannot_be_queried(self):
        self.lake.publish('import-000000',[self.row()])
        with self.assertRaises(ValueError):
            self.lake.commits()

if __name__=='__main__':
    unittest.main()

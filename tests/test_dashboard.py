import copy
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest
from dashboard.data import scope, totals, batch_history


def fixture():
    quality = pd.DataFrame([
        dict(event_date='2026-06-06', replay_run_id='run-a', raw_count=2, silver_count=2, rejected_count=0, null_timestamp_count=0),
        dict(event_date='unknown', replay_run_id='run-b', raw_count=1, silver_count=1, rejected_count=0, null_timestamp_count=1),
        dict(event_date='unknown', replay_run_id=None, raw_count=1, silver_count=0, rejected_count=1, null_timestamp_count=0)])
    silver = pd.DataFrame([dict(event_date=d, replay_run_id=r, event_time=t, source_id=str(i), event_type='event',
                               attributes_event_type='screen', attributes_channel='web', application_version_name='1.0')
                          for i,(d,r,t) in enumerate([('2026-06-06','run-a','2026-06-07T04:00:00Z'),('2026-06-06','run-a','2026-06-07T04:00:00Z'),('unknown','run-b',None)])])
    return dict(lake_id='m4-gce-june', bucket='test', loaded_at='2026-09-14T00:00:00Z',
        gold=dict(updated_at='2026-09-10T00:00:00Z',snapshot_id='test',timezone='America/Los_Angeles',processed_ranges=[]),
        quality=quality, volume=quality[quality.silver_count>0][['event_date','replay_run_id','silver_count']].rename(columns={'silver_count':'event_count'}),
        reasons=pd.DataFrame([dict(event_date='unknown',replay_run_id=None,reason='MALFORMED_JSON',event_count=1)]),
        silver=silver, rejected=pd.DataFrame([dict(event_date='unknown',replay_run_id=None,reason='MALFORMED_JSON')]),
        newer_commits=0, files=[], history=[], kafka=dict(available=False,partitions=[]),replay=[],comparison=None)


class DashboardTests(unittest.TestCase):
    def test_run_date_and_unknown_scope(self):
        data=fixture()['quality']
        self.assertEqual(totals(scope(data,run='run-a'))['raw_count'],2)
        self.assertEqual(totals(scope(data,date_scope='Unknown only'))['raw_count'],2)
        self.assertEqual(totals(scope(data,unidentified=True))['rejected_count'],1)
        self.assertEqual(totals(scope(data,start=date(2026,6,7),end=date(2026,6,8),date_scope='Dated only'))['raw_count'],0)
        self.assertEqual(totals(scope(data,start=date(2026,6,7),end=date(2026,6,8)))['raw_count'],2)
    def test_weighted_rates_and_empty_denominators(self):
        data=fixture()['quality']
        self.assertEqual(totals(data)['null_ratio'],1/3)
        self.assertEqual(totals(data)['rejected_ratio'],1/4)
        self.assertIsNone(totals(data.iloc[:0])['null_ratio'])
    def test_spark_rereads_not_double_counted(self):
        import json
        row=dict(id='q',batchId=1,timestamp='now',numInputRows=40,durationMs=dict(triggerExecution=2000),
                 sources=[dict(startOffset={'topic':{'0':10}},endOffset={'topic':{'0':30}})])
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'progress.jsonl';path.write_text(json.dumps(row)+'\n'+json.dumps(row)+'\n{incomplete')
            values=batch_history(path)
        self.assertEqual(len(values),1)
        self.assertEqual(values[0]['source_offsets'],20)
        self.assertEqual(values[0]['offsets_per_second'],10)
    def test_manual_refresh_and_filter_restore(self):
        with patch('dashboard.data.load_snapshot',return_value=fixture()) as loader:
            app=AppTest.from_file('dashboard/app.py',default_timeout=20).run()
            self.assertFalse(app.exception)
            self.assertEqual(app.metric[0].value,'4')
            app.sidebar.selectbox[0].select(1).run()
            self.assertEqual(app.metric[0].value,'2')
            app.sidebar.selectbox[0].select(0).run()
            self.assertEqual(app.metric[0].value,'4')
            self.assertEqual(loader.call_count,1)
            app.sidebar.button[0].click().run()
            self.assertEqual(loader.call_count,2)
            self.assertFalse(app.exception)
    def test_failed_refresh_retains_snapshot(self):
        with patch('dashboard.data.load_snapshot',side_effect=[fixture(),OSError('private service detail')]):
            app=AppTest.from_file('dashboard/app.py',default_timeout=20).run()
            app.sidebar.button[0].click().run()
            self.assertFalse(app.exception)
            self.assertEqual(app.metric[0].value,'4')
            self.assertTrue(app.error)
            self.assertNotIn('private service detail',app.error[0].value)
    def test_initial_unavailable(self):
        with patch('dashboard.data.load_snapshot',side_effect=OSError('secret')):
            app=AppTest.from_file('dashboard/app.py',default_timeout=20).run()
            self.assertFalse(app.exception)
            self.assertTrue(app.error)
            self.assertEqual(len(app.metric),0)
    def test_empty_selection(self):
        with patch('dashboard.data.load_snapshot',return_value=fixture()):
            app=AppTest.from_file('dashboard/app.py',default_timeout=20).run()
            app.sidebar.selectbox[0].select(1).run()
            app.sidebar.selectbox[1].select('Unknown only').run()
            self.assertFalse(app.exception)
            self.assertEqual(app.metric[0].value,'0')
            self.assertEqual(app.metric[3].value,'N/A')
    def test_new_data_only_after_refresh(self):
        newer=copy.deepcopy(fixture())
        newer['quality'].loc[0,'raw_count']=3
        with patch('dashboard.data.load_snapshot',side_effect=[fixture(),newer]):
            app=AppTest.from_file('dashboard/app.py',default_timeout=20).run()
            self.assertEqual(app.metric[0].value,'4')
            app.sidebar.button[0].click().run()
            self.assertEqual(app.metric[0].value,'5')


if __name__=='__main__':
    unittest.main()

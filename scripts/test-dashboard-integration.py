#!/usr/bin/env python3
"""Read the real lake once, then verify the Streamlit UI and manual refresh contract."""
import json
from pathlib import Path
import sys
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from streamlit.testing.v1 import AppTest
from dashboard.data import load_snapshot, totals


def main():
    snapshot=load_snapshot('m4-gce-june')
    expected=totals(snapshot['quality'])
    with patch('dashboard.data.load_snapshot',return_value=snapshot) as loader:
        app=AppTest.from_file('dashboard/app.py',default_timeout=30).run()
        assert not app.exception,app.exception
        assert [x.value for x in app.metric[:3]]==[f"{expected[k]:,}" for k in ('raw_count','silver_count','rejected_count')]
        app.sidebar.selectbox[0].select(1).run()
        run=sorted(snapshot['quality'].replay_run_id.dropna().unique())[0]
        assert app.metric[0].value==f"{int(snapshot['quality'].loc[snapshot['quality'].replay_run_id==run,'raw_count'].sum()):,}"
        app.sidebar.selectbox[0].select(0).run()
        assert app.metric[0].value==f"{expected['raw_count']:,}"
        app.sidebar.selectbox[1].select('Unknown only').run()
        unknown=int(snapshot['quality'].loc[snapshot['quality'].event_date=='unknown','raw_count'].sum())
        assert app.metric[0].value==f'{unknown:,}'
        app.sidebar.button[1].click().run()
        assert app.metric[0].value==f"{expected['raw_count']:,}"
        assert loader.call_count==1
        next(s for s in app.selectbox if s.label=='Event classification field').select('attributes_event_type').run()
        assert not app.exception,app.exception
        app.sidebar.button[0].click().run()
        assert loader.call_count==2
        assert not app.exception,app.exception
    result=dict(status='PASS',counts=expected,kafka=snapshot['kafka'],snapshot_id=snapshot['gold']['snapshot_id'],
        run_filter=True,unknown_dates=True,reset_filters=True,manual_refresh_only=True,classification_toggle=True,apptest_exceptions=0)
    Path('data/m5').mkdir(exist_ok=True)
    Path('data/m5/acceptance.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':
    main()

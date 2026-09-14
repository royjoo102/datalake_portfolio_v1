"""English, manually refreshed M5 dashboard. Run through scripts/run-dashboard.sh."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import altair as alt
import pandas as pd
import streamlit as st
from dashboard.data import load_snapshot, scope, totals, breakdown

st.set_page_config(page_title='Data lake overview', page_icon='📊', layout='wide')
st.title('Data lake overview')

with st.sidebar:
    st.header('Data source')
    lake_id = st.text_input('Lake ID', value=os.getenv('DASHBOARD_LAKE_ID', 'm4-gce-june'))
    refresh = st.button('Refresh data', type='primary', width='stretch')
    st.caption('Loads published data. Does not run Replay or Spark.')

if 'snapshot' not in st.session_state or refresh:
    try:
        with st.spinner('Reading the published snapshot…'):
            candidate = load_snapshot(lake_id)
        st.session_state.snapshot = candidate
        st.session_state.pop('refresh_error', None)
    except Exception as exc:
        # Service errors can include URLs or request details; do not expose credentials.
        st.session_state.refresh_error = str(exc) if isinstance(exc, ValueError) else 'Unable to load data. Check MinIO and the published M4 output, then refresh.'

if 'refresh_error' in st.session_state:
    st.error(st.session_state.refresh_error)
if 'snapshot' not in st.session_state:
    st.stop()
snap = st.session_state.snapshot
if 'refresh_error' in st.session_state:
    st.warning('Showing the previous successful snapshot. The failed refresh did not replace it.')
if lake_id != snap['lake_id']:
    st.info(f"Showing {snap['lake_id']}. Select Refresh data to load the entered Lake ID.")

def display_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(ZoneInfo('America/Los_Angeles')).strftime('%b %d, %Y %H:%M %Z')


st.caption(f"Lake: {snap['lake_id']} · Gold updated: {display_time(snap['gold']['updated_at'])} · Loaded: {display_time(snap['loaded_at'])}")
if snap['newer_commits']:
    st.warning(f"{snap['newer_commits']} newer committed batch(es) are not yet included in Gold. All event views below use the published Gold snapshot.")

with st.sidebar:
    st.header('Event filters')
    runs = sorted(snap['quality'].replay_run_id.dropna().unique())
    choices = [None] + runs
    has_unknown_run = snap['quality'].replay_run_id.isna().any()
    labels = ['All runs'] + runs + (['Unidentified run'] if has_unknown_run else [])
    if st.button('Reset event filters'):
        for key in ('run_filter', 'date_coverage', 'event_dates'):
            st.session_state.pop(key, None)
    run_index = st.selectbox('Replay run', list(range(len(labels))), format_func=lambda i: labels[i], key='run_filter')
    unidentified = has_unknown_run and run_index == len(choices)
    run = None if unidentified else choices[run_index]
    dates = sorted(set(snap['quality'].event_date)-{'unknown'})
    date_scope = st.selectbox('Date coverage', ['Dated + unknown', 'Dated only', 'Unknown only'], key='date_coverage')
    start, end = None, None
    if dates:
        selected = st.date_input('Event date range (Los Angeles)', (date.fromisoformat(dates[0]), date.fromisoformat(dates[-1])), key='event_dates')
        if len(selected) != 2:
            st.info('Select a start and end date.')
            st.stop()
        start, end = selected
    st.caption('Unknown dates are independent of the selected date range. Business deduplication is not applied.')

filters = dict(start=start, end=end, date_scope=date_scope, run=run, unidentified=unidentified)
quality, volume, reasons, silver, rejected = [scope(snap[key], **filters) for key in ('quality', 'volume', 'reasons', 'silver', 'rejected')]
counts = totals(quality)
cols = st.columns(4)
for col, label, key in zip(cols[:3], ['Raw events', 'Silver events', 'Rejected events'], ['raw_count', 'silver_count', 'rejected_count']):
    col.metric(label, f'{counts[key]:,}')
cols[3].metric('Rejected / Raw', 'N/A' if counts['rejected_ratio'] is None else f"{counts['rejected_ratio']:.2%}")
if not counts['raw_count']:
    st.info('No events match the selected filters.')


def bars(frame, category='Category', value='Events', horizontal=True):
    if frame.empty:
        st.info('No data for this selection.')
        return
    if horizontal:
        chart = alt.Chart(frame).mark_bar(color='#2563a6').encode(
            x=alt.X(f'{value}:Q', title=value), y=alt.Y(f'{category}:N', sort='-x', title=None, axis=alt.Axis(labelOverlap=False, labelLimit=180)),
            tooltip=[category, value]).properties(height=max(160, min(480, len(frame)*28)))
    else:
        chart = alt.Chart(frame).mark_bar(color='#2563a6').encode(
            x=alt.X(f'{category}:O', title='Event date (Los Angeles)'), y=alt.Y(f'{value}:Q', title=value),
            tooltip=[category, value]).properties(height=280)
    st.altair_chart(chart, use_container_width=True)


def table(frame):
    st.dataframe(frame, hide_index=True, width='stretch')


def serializable(frame):
    result = frame.copy()
    for column in result.select_dtypes('object'):
        result[column] = result[column].map(lambda v: json.dumps(v.tolist() if hasattr(v, 'tolist') else v, ensure_ascii=False)
            if isinstance(v, (list, dict)) or hasattr(v, 'tolist') else v)
    return result


overview, explorer, pipeline, storage = st.tabs(['Overview', 'Event explorer', 'Pipeline', 'Storage'])
with overview:
    st.subheader('Daily event volume')
    daily = volume[volume.event_date != 'unknown'].groupby('event_date', as_index=False).event_count.sum()
    bars(daily.rename(columns={'event_date':'Date', 'event_count':'Events'}), 'Date', horizontal=False)
    st.caption(f"Silver events with unknown dates: {int(volume.loc[volume.event_date == 'unknown', 'event_count'].sum()):,}. Dates without stored events are not shown.")
    a, b = st.columns([1, 2])
    with a:
        st.subheader('Timestamp quality')
        st.metric('Null Silver timestamps', f"{counts['null_timestamp_count']:,}")
        st.metric('Null timestamps / Silver', 'N/A' if counts['null_ratio'] is None else f"{counts['null_ratio']:.2%}")
        st.caption('Missing or invalid timestamps are retained in Silver with a quality flag.')
    with b:
        st.subheader('Rejection reasons')
        reason_counts = reasons.groupby('reason', as_index=False).event_count.sum().rename(columns={'reason':'Category','event_count':'Events'})
        bars(reason_counts)
        st.caption('An event may have multiple reasons; reason counts are not additive to the Rejected total.')
    with st.expander('Rejected event details'):
        table(serializable(rejected))
    with st.expander('Daily counts and definitions'):
        table(quality)
        st.caption('Counts are grouped by LA event date and replay run. Rates use summed numerators and denominators, not averages of daily rates. Unidentified runs remain included in All runs.')

with explorer:
    st.subheader('Explore Silver events')
    st.caption('Date and replay-run filters apply. The controls below affect only this explorer.')
    event_field = st.selectbox('Event classification field', ['event_type', 'attributes_event_type'])
    channel_values = sorted(silver.attributes_channel.dropna().unique())
    channel_index = st.selectbox('Channel', list(range(len(channel_values)+1)), format_func=lambda i: 'All channels' if i == 0 else channel_values[i-1] or '(Empty)')
    explored = silver if channel_index == 0 else silver[silver.attributes_channel == channel_values[channel_index-1]]
    st.caption(f'{len(explored):,} matching Silver events')
    a, b = st.columns(2)
    with a:
        st.markdown('**Event classification**')
        bars(breakdown(explored, event_field).head(20))
        st.caption('Top 20 categories by event count.')
    with b:
        st.markdown('**Application version**')
        bars(breakdown(explored, 'application_version_name').head(20))
        st.caption('Top 20 application versions by event count.')
    st.markdown('**Channel distribution**')
    bars(breakdown(explored, 'attributes_channel').head(20))
    limit = st.selectbox('Rows to display', [100, 500, 1000])
    columns = st.multiselect('Columns', list(explored.columns), default=['event_date','event_time','event_type','attributes_event_type','attributes_channel','application_version_name','replay_run_id','source_id'])
    shown = serializable(explored.sort_values(['event_time','source_id'], na_position='last').head(limit)[columns])
    table(shown)
    st.caption(f'Showing {len(shown):,} of {len(explored):,} rows, ordered by event time (UTC).')
    st.download_button('Download displayed rows (CSV)', shown.to_csv(index=False).encode('utf-8-sig'), 'silver-selection.csv', 'text/csv', on_click='ignore')

with pipeline:
    st.subheader('Pipeline status')
    st.caption('Lake-wide operational measurements at the last manual refresh. Event filters do not apply here.')
    st.info('M4 uses availableNow: each invocation drains its captured Kafka range and exits. No always-on consumer is configured.')
    if snap['kafka']['available']:
        table(pd.DataFrame(snap['kafka']['partitions']))
        st.caption('Offset gap = Kafka end offset − MinIO committed end offset. This is an offset distance, not a Kafka consumer-group lag or a guaranteed event count. Negative values indicate a source reset or mismatch.')
    else:
        st.warning('Kafka status unavailable. Stored event data remains queryable.')
    st.markdown('**Recent Spark batches**')
    history = pd.DataFrame(snap['history'])
    table(history)
    if not history.empty:
        bars(history[['batch','duration_ms']].rename(columns={'batch':'Batch','duration_ms':'Duration (ms)'}), 'Batch', 'Duration (ms)')
    st.caption('Throughput uses Kafka offset advancement / trigger duration. Spark numInputRows can count rereads and is not used as a unique event count. Historical event timestamps are not used as processing latency.')
    st.markdown('**Replay checkpoints**')
    table(pd.DataFrame(snap['replay']))
    st.caption('Last acknowledged CSV row and end-of-file status. A completion percentage requires a verified total CSV row count. Live process activity and end-to-end processing latency are not instrumented.')

with storage:
    st.subheader('Published storage')
    st.caption('Whole Gold snapshot coverage; date and run filters do not change physical file sizes. Older Gold versions and uncommitted uploads are excluded.')
    files = pd.DataFrame(snap['files'])
    if not files.empty:
        sizes = files.groupby('zone', as_index=False).agg(files=('key','count'), bytes=('bytes','sum'), min_bytes=('bytes','min'), median_bytes=('bytes','median'), max_bytes=('bytes','max'))
        table(sizes)
        bars(sizes.rename(columns={'zone':'Zone','bytes':'Bytes'}), 'Zone', 'Bytes')
        st.caption(f"{int((files['bytes'] < 1024**2).sum())} of {len(files)} files are under 1 MiB. No automatic compaction or deletion is applied.")
    comparison = snap.get('comparison')
    if comparison and comparison.get('fair_parquet_comparison'):
        fair = comparison['fair_parquet_comparison']
        st.markdown('**Business-column comparison — M4 validation baseline**')
        compare = pd.DataFrame({'Columns':[str(fair['full_columns']),str(fair['selected_columns'])], 'Bytes':[fair['full_bytes'], fair['selected_bytes']]})
        bars(compare, 'Columns', 'Bytes')
        st.caption(f"{fair['rows']:,} identical baseline rows · {fair['compression']} compression · {fair['row_group_size']:,}-row groups · {fair['reduction_percent']}% smaller. Excludes technical fields and does not measure total lake savings.")
    else:
        st.info('No verified column-size comparison is available for this lake.')
    with st.expander('Snapshot source and coverage'):
        st.json({k:snap['gold'][k] for k in ('snapshot_id','updated_at','timezone','processed_ranges')})
        st.caption(f"Source: s3://{snap['bucket']}/_control/{snap['lake_id']}/gold/latest.json")

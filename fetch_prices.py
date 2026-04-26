#!/usr/bin/env python3
"""
Fetch real historical oil prices from Yahoo Finance and build a prices.json
data file for the oil-dashboard. No API key needed — uses public Yahoo
Finance v8 chart endpoint.

Usage: python3 fetch_prices.py
then commit data/prices.json
"""
import json, urllib.request, urllib.error, os, sys, datetime

def fetch_yahoo(ticker):
    """Fetch chart data for a Yahoo Finance ticker."""
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2y'
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; data-fetcher/1.0)'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode())
    result = data['chart']['result'][0]
    timestamps = result['timestamp']
    quote = result['indicators']['quote'][0]
    return timestamps, quote

def build_records(ts_bz, q_bz, ts_cl, q_cl):
    """Align Brent and WTI into daily records."""
    # Index WTI by timestamp for fast lookup
    cl_idx = {}
    for t, close in zip(ts_cl, q_cl['close']):
        if close is not None and t is not None:
            cl_idx[int(t)] = close

    records = []
    for t, brent_close in zip(ts_bz, q_bz['close']):
        if t is None or brent_close is None:
            continue
        t = int(t)
        wti_close = cl_idx.get(t)
        if wti_close is None:
            # Try nearest +/- 1 day WTI
            for delta in (-1, 1, -2, 2):
                wti_close = cl_idx.get(t + delta * 86400)
                if wti_close:
                    break
        if wti_close is None:
            continue
        dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
        records.append({
            'date': dt.strftime('%Y-%m-%d'),
            'brent': round(brent_close, 2),
            'wti': round(wti_close, 2),
            'spread': round(brent_close - wti_close, 2),
        })
    return records

def fetch_live_snapshot(ticker):
    """Get the latest price snapshot for a ticker."""
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d'
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; data-fetcher/1.0)'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode())
    meta = data['chart']['result'][0]['meta']
    return meta.get('regularMarketPrice')

def main():
    print('Fetching Brent (BZ=F)...')
    ts_bz, q_bz = fetch_yahoo('BZ=F')
    print(f'  Got {len(ts_bz)} days')

    print('Fetching WTI (CL=F)...')
    ts_cl, q_cl = fetch_yahoo('CL=F')
    print(f'  Got {len(ts_cl)} days')

    print('Building records...')
    records = build_records(ts_bz, q_bz, ts_cl, q_cl)
    print(f'  Aligned {len(records)} days')

    latest_date = records[-1]['date'] if records else None
    brent_live = fetch_live_snapshot('BZ=F')
    wti_live = fetch_live_snapshot('CL=F')

    output = {
        'meta': {
            'source': 'Yahoo Finance (BZ=F = ICE Brent Futures, CL=F = NYMEX WTI Futures)',
            'lastUpdated': datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
            'latestDate': latest_date,
            'liveBrent': brent_live,
            'liveWti': wti_live,
        },
        'prices': records,
    }

    os.makedirs('data', exist_ok=True)
    with open('data/prices.json', 'w') as fh:
        json.dump(output, fh, indent=2)

    print(f'Saved {len(records)} records to data/prices.json')
    print(f'  Latest date: {latest_date}')
    print(f'  Live Brent: {brent_live} | Live WTI: {wti_live}')

if __name__ == '__main__':
    main()

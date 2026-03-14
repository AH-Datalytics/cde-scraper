"""
FBI Crime Data Explorer — SRS Agency Scraper (async)
Pulls summarized agency-level crime data for RTCI ORIs.

Usage:
    python scraper.py                         # 2017 to current month
    python scraper.py --from 01-2023          # custom start
    python scraper.py --to 12-2024            # custom end
    python scraper.py --concurrency 20        # more parallel requests
    python scraper.py --output my_file.csv    # custom filename
"""

import asyncio
import aiohttp
import argparse
import csv
import time
import os
from datetime import datetime
from collections import defaultdict

# ─── Configuration ───────────────────────────────────────────────────────────

API_KEY = os.environ.get("CDE_API_KEY", "BPkjHOgf6hpOoYlRm7GOaHbSQqlx87IfiXP3QTJg")
BASE = "https://cde.ucr.cjis.gov/LATEST"

OFFENSE_TYPES = [
    "homicide",
    "rape",
    "robbery",
    "aggravated-assault",
    "burglary",
    "larceny",
    "motor-vehicle-theft",
]

STATE_NAMES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
    "District of Columbia",
}

ORI_LIST = [
    "AK0010100","AL0010000","AL0010200","AL0011200","AL0020000","AL0020100","AL0030100","AL0050000",
    "AL0380100","AL0430100","AL0470000","AL0470100","AL0470200","AL0520100","AL0630100","AR0040100",
    "AR0040200","AR0160100","AR0230100","AR0600200","AR0600300","AR0660100","AR0720100","AR0720200",
    "AZ0030100","AZ0070000","AZ0070100","AZ0070300","AZ0070500","AZ0071100","AZ0071300","AZ0071500",
    "AZ0071700","AZ0072100","AZ0072300","AZ0072500","AZ0072700","AZ0072900","AZ0080400","AZ0100000",
    "AZ0100300","AZ0100900","AZ0110000","AZ0110100","AZ0111700","AZ0130000","AZ0131100","AZ0140500",
    "CA0010000","CA0010100","CA0010300","CA0010500","CA0010600","CA0010700","CA0010900","CA0011100",
    "CA0011200","CA0011300","CA001300X","CA0040200","CA0070000","CA0070100","CA0070200","CA0070400",
    "CA0070800","CA0071000","CA0071200","CA0090000","CA0100000","CA0100100","CA0100500","CA0150000",
    "CA0150200","CA0150300","CA0160200","CA0190000","CA0190100","CA0190200","CA0190600","CA0190800",
    "CA0191200","CA0191500","CA0191R0X","CA0191W0X","CA0192000","CA0192200","CA0192400","CA0192500",
    "CA0192600","CA0192800","CA0193100","CA0193300","CA0193500","CA0194100","CA0194200","CA0194300",
    "CA0194700","CA0194800","CA0194900","CA0195000","CA0195200","CA0195300","CA0195400","CA0195500",
    "CA0195600","CA0196500","CA0196900","CA0197200","CA0197500","CA0197600","CA0197700","CA019960X",
    "CA0200200","CA0210600","CA0210900","CA0240600","CA0270000","CA0270800","CA0280200","CA0300000",
    "CA0300100","CA0300300","CA0300400","CA0300700","CA0300800","CA0300900","CA0301000","CA0301200",
    "CA0301400","CA0301500","CA0301600","CA0301700","CA0301900","CA0302200","CA0302400","CA0302500",
    "CA0302600","CA030350X","CA030390X","CA030430X","CA030490X","CA0310000","CA0310300","CA0310400",
    "CA0310500","CA0330000","CA0330200","CA0330800","CA0330900","CA0331200","CA0331300","CA0331400",
    "CA0331500","CA0331800","CA0332500","CA033300X","CA033320X","CA033380X","CA0334200","CA033790X",
    "CA033A000","CA0340000","CA0340100","CA0340400","CA0340H00","CA034550X","CA0349600","CA0360000",
    "CA0360200","CA0360300","CA0360400","CA0360700","CA0360800","CA0360900","CA0361000","CA0361100",
    "CA0361200","CA0361600","CA036300X","CA036320X","CA036330X","CA036350X","CA036400X","CA0370000",
    "CA0370100","CA0370200","CA0370500","CA0370600","CA0370800","CA0370900","CA0371000","CA0371100",
    "CA0371200","CA0371300","CA037A200","CA037A400","CA0380100","CA0390000","CA0390200","CA0390300",
    "CA0390500","CA0390600","CA0400000","CA0410000","CA0410600","CA0411300","CA0411600","CA0411700",
    "CA0420000","CA0420300","CA0420400","CA0430000","CA0430300","CA0430400","CA0430800","CA0431100",
    "CA0431200","CA0431300","CA0431400","CA0431600","CA0440000","CA0440200","CA0440300","CA0450200",
    "CA0480300","CA0480600","CA0480700","CA0490000","CA0490500","CA0490800","CA0500000","CA0500200",
    "CA0500700","CA0510200","CA0540000","CA0540500","CA0540600","CA0540700","CA0560000","CA0560100",
    "CA0560400","CA0560700","CA0560800","CA0560900","CA0570100","CA0570300","CA0570400","CO0010000",
    "CO0010100","CO0010300","CO0010400","CO0010500","CO0030000","CO0031100","CO0070100","CO0070400",
    "CO0180000","CO0180100","CO0180500","CO0210000","CO0210100","CO0300000","CO0300100","CO0300400",
    "CO0350300","CO0350400","CO0390100","CO0510100","CO0620200","CO0640100","CODPD0000","CT0001500",
    "CT0001700","CT0003400","CT0004300","CT0005100","CT0005700","CT0006200","CT0006400","CT0007700",
    "CT0008000","CT0008400","CT0008900","CT0009300","CT0010300","CT0013500","CT0013800","CT0015100",
    "CT0015500","CT0015600","DCMPD0000","DE0020300","DE0020600","FL0010000","FL0010100","FL0030000",
    "FL0050000","FL0050700","FL0051200","FL0060200","FL0060300","FL0060500","FL0060600","FL0060700",
    "FL0060800","FL0061100","FL0061200","FL0061800","FL0062100","FL0062200","FL0062700","FL0062800",
    "FL0063100","FL006880X","FL0080000","FL0090000","FL0100000","FL0110000","FL0130000","FL0130400",
    "FL0130500","FL0130600","FL0130700","FL0131100","FL0131800","FL0139700","FL0160000","FL0170000",
    "FL0170100","FL0180000","FL0270000","FL0290000","FL0290200","FL0310000","FL0350000","FL0350300",
    "FL0360000","FL0360100","FL0360200","FL0370300","FL0410000","FL0410200","FL0420000","FL0420100",
    "FL0430000","FL0460000","FL0480000","FL0480100","FL0480400","FL0490000","FL0490100","FL0490200",
    "FL0500000","FL0500200","FL0500300","FL0500400","FL0500800","FL0501700","FL0502600","FL050950X",
    "FL0510000","FL0520000","FL0520300","FL0520800","FL0521100","FL0521400","FL0530000","FL0531200",
    "FL0531600","FL0550000","FL0560200","FL0570000","FL0580000","FL0580100","FL0580300","FL0590000",
    "FL0590500","FL0600000","FL0640000","FL0640100","FL0641200","GA0110000","GA0250300","GA0280000",
    "GA0290100","GA0310100","GA0330200","GA0330300","GA0330400","GA0360000","GA0380000","GA0440200",
    "GA0447100","GA0447200","GA0470100","GA0480000","GA0580000","GA0600400","GA0600500","GA0605600",
    "GA0605800","GA0670200","GA0690000","GA0750500","GA0760200","GA0920100","GA1060100","GA1070000",
    "GA1100000","GA1210000","GAAPD0000","HI0010000","HI0020000","HI0050000","IA0070300","IA0310100",
    "IA0520200","IA0570100","IA0770100","IA0770300","IA0770500","IA0780100","IA0820200","IA0850100",
    "IA0970100","ID0010000","ID0010100","ID0010300","ID0030200","ID0100200","ID0140100","ID0140200",
    "ID0280100","ID0420200","IL0100100","IL0160000","IL0160200","IL0160900","IL0162100","IL0162500",
    "IL0162A00","IL0163200","IL0164A00","IL0165M00","IL0167200","IL0168000","IL0168100","IL0168300",
    "IL0168400","IL0220000","IL0221400","IL0222100","IL0450100","IL0450600","IL0490000","IL0492100",
    "IL0570100","IL0570200","IL0580200","IL0720700","IL0840200","IL0990000","IL0990200","IL0990700",
    "IL1010400","ILCPD0000","IN0020000","IN0020100","IN0030100","IN0100300","IN0180100","IN0200000",
    "IN0200100","IN0290100","IN0290200","IN0290400","IN0290700","IN0340100","IN0410300","IN0450500",
    "IN0450700","IN0480200","IN0530100","IN0710000","IN0710100","IN0710200","IN0790100","IN0820100",
    "IN0840100","INIPD0000","KS0230100","KS0460500","KS0460600","KS0460900","KS0461000","KS0870300",
    "KS0890100","KS1050200","KY0080000","KY0300100","KY0340200","KY0568000","KY1140100","LA0030000",
    "LA0080100","LA0090100","LA0100200","LA0170000","LA0170200","LA0260000","LA0260300","LA0280300",
    "LA0320000","LA0520000","LA0530000","LANPD0000","MA0010100","MA0022200","MA0030800","MA0031100",
    "MA0031900","MA0051100","MA0051300","MA0051400","MA0051900","MA0052500","MA0070500","MA0071800",
    "MA0091100","MA0091700","MA0091800","MA0092600","MA0092700","MA0093000","MA0093300","MA0093900",
    "MA0094700","MA0110400","MA0112000","MA0112700","MA0120300","MA0122000","MA0130100","MA0130400",
    "MA0146000","MD0020200","MD0030100","MD0090000","MD0110000","MD0110300","MD0130000","MD0140100",
    "MD0160200","MD0160400","MD0160500","MD0172100","MD0174100","MD0190000","MD0220000","MDBPD0000",
    "ME0030500","MI1323700","MI2539800","MI3336400","MI3351900","MI3913900","MI3949900","MI4114100",
    "MI4143600","MI4183400","MI4185000","MI4714700","MI5015000","MI5072200","MI5074000","MI5076500",
    "MI5080600","MI5084900","MI5815800","MI631900X","MI631910X","MI6338900","MI6362700","MI6371400",
    "MI6375100","MI6378400","MI6380800","MI6381500","MI7017000","MI7417400","MI8118100","MI8121800",
    "MI8234300","MI8234400","MI8234900","MI8253800","MI8277500","MI8281700","MI8290800","MN0020200",
    "MN0020500","MN0190100","MN0190800","MN0191000","MN0191100","MN0270100","MN0270300","MN0270600",
    "MN0271100","MN0271200","MN0271700","MN0272600","MN0272700","MN0550100","MN0620900","MN0690600",
    "MN0730400","MN0821100","MN0860000","MO0100200","MO0110100","MO0390300","MO0480100","MO0480600",
    "MO0480800","MO0490700","MO0500000","MO0920100","MO0920300","MO0920400","MO0922400","MO0950000",
    "MO0953000","MOKPD0000","MOSPD0000","MS0170100","MS0240200","MS0250100","MT0070100","MT0160100",
    "MT0320100","MT0560100","NC0010100","NC0110000","NC0110100","NC0130100","NC0130200","NC0260000",
    "NC0260100","NC0290000","NC0320100","NC0330100","NC0340000","NC0340200","NC0360600","NC0410000",
    "NC0410200","NC0410300","NC0430000","NC0490000","NC0490100","NC0510000","NC0600100","NC0600600",
    "NC0650000","NC0650200","NC0670000","NC0670100","NC0680100","NC0740300","NC0900000","NC0920000",
    "NC0920100","NC0920200","NC0920300","NC0920700","ND0080100","ND0090200","ND0180100","NB0280000",
    "NB0280200","NB0400100","NB0550100","NB0770000","NB0770100","NH0063400","NH0064600","NJ0041200",
    "NJ0041500","NJ0061400","NJ0070200","NJ0070600","NJ0070900","NJ0081800","NJ0090100","NJ0090500",
    "NJ0090600","NJ0090800","NJ0091000","NJ0091200","NJ0110300","NJ0111100","NJ0120400","NJ0120500",
    "NJ0120900","NJ0121400","NJ0121600","NJ0121700","NJ0122500","NJ0131900","NJ0133100","NJ0142900",
    "NJ0150600","NJ0150700","NJ0151100","NJ0151400","NJ0160200","NJ0160700","NJ0160800","NJ0161400",
    "NJ0180800","NJ0200400","NJ0201200","NJ0201900","NJNPD0000","NM0010000","NM0010100","NM0070100",
    "NM0230600","NM0260100","NV0020100","NV0020200","NV0020300","NV0160000","NV0160100","NV0160200",
    "NY0010100","NY0015300","NY0130000","NY0140000","NY0140100","NY0145100","NY0145500","NY0147200",
    "NY0270000","NY0270100","NY0275400","NY0290000","NY0290600","NY0303000","NY0320200","NY0330000",
    "NY0330100","NY0410200","NY0435000","NY0435300","NY0450000","NY0460100","NY0510100","NY0515800",
    "NY0590200","NY0590300","NY0590400","NY0590700","OH0090200","OH0090300","OH0091500","OH0120200",
    "OH0131600","OH0184200","OH0210000","OH0310000","OH0314200","OH0314400","OH0450100","OH0470400",
    "OH0470500","OH0480700","OH0500900","OH0570200","OH0570500","OH0760000","OH0760400","OH0770100",
    "OH0770300","OH0830000","OHCIP0000","OHCLP0000","OHCOP0000","OK0140100","OK0140200","OK0160100",
    "OK0240100","OK0550300","OK0550400","OK0550600","OK0720100","OK0720500","OR0020100","OR0030000",
    "OR0090100","OR0150400","OR0200000","OR0200200","OR0200600","OR0220100","OR0240000","OR0240200",
    "OR0260100","OR0260200","OR0340000","OR0340100","OR0340300","OR0340400","PA0061400","PA0090100",
    "PA0090300","PA0140300","PA0220200","PA0220400","PA0231400","PA0233700","PA0250200","PA0250300",
    "PA0350400","PA0360500","PA0360800","PA0390100","PA0460100","PA0461400","PA0480300","PA0671500",
    "PAPEP0000","PAPPD0000","RI0020300","RI0040200","RI0040800","RI0040900","SC0020000","SC0040000",
    "SC0070000","SC0080000","SC0080300","SC0100100","SC0100300","SC0100800","SC0180200","SC0230000",
    "SC0230200","SC0260400","SC0290000","SC0320000","SC0400000","SC0400100","SC0420000","SC0460000",
    "SC0460300","SD0490200","SD0510100","TN0190100","TN0330000","TN0330100","TN0470000","TN0470100",
    "TN0570100","TN0600300","TN0630100","TN0750000","TN0750100","TN0750200","TN0790000","TN0790100",
    "TN0790600","TN0820200","TN0830100","TN0830400","TN0900100","TN0940100","TN0950100","TNMPD0000",
    "TX0140400","TX0140700","TX0150000","TX0200000","TX0201000","TX0210100","TX0210200","TX0310000",
    "TX0310100","TX0310300","TX0430000","TX0430100","TX0430400","TX0430500","TX0430600","TX0430800",
    "TX0460000","TX0460100","TX0570400","TX0570800","TX0571100","TX0571200","TX0571500","TX0571800",
    "TX0572000","TX0573300","TX0610000","TX0610200","TX0610600","TX0611200","TX0611300","TX0680200",
    "TX0710000","TX0710200","TX0790000","TX0790100","TX0790500","TX0840400","TX0840800","TX0840900",
    "TX0920500","TX1010000","TX1010100","TX1011500","TX1050000","TX1050100","TX1050700","TX1080000",
    "TX1080400","TX1080800","TX1081000","TX1081100","TX1230100","TX1230700","TX1260200","TX1290000",
    "TX1520200","TX1551200","TX1650100","TX1700000","TX1700100","TX1780200","TX1840000","TX1880100",
    "TX1990100","TX2120000","TX2120400","TX2200100","TX2200900","TX2201200","TX2201300","TX2202000",
    "TX2202100","TX2210100","TX2260100","TX2270000","TX2270100","TX2270900","TX2350100","TX2400100",
    "TX2430500","TX2460000","TX2460200","TX2460500","TX2460900","TX2461700","TXDPD0000","TXHPD0000",
    "TXSPD0000","UT0030100","UT0060300","UT0180000","UT0180300","UT0180500","UT0180600","UT0181200",
    "UT0181800","UT0182500","UT0183900","UT0250100","UT0250200","UT0250300","UT0250600","UT0252200",
    "UT0270100","UT0290100","VA0020300","VA0070100","VA0210100","VA0290100","VA0420000","VA0430100",
    "VA0530000","VA0750300","VA0880000","VA0890000","VA0990000","VA1030000","VA1110000","VA1120000",
    "VA1140000","VA1160000","VA1170000","VA1200000","VA1220000","VA1230000","VA1270000","VA1280000",
    "VT0040100","WA0030100","WA0030200","WA0060000","WA0060300","WA0110200","WA0170000","WA0170100",
    "WA0170200","WA0170300","WA0170700","WA0170800","WA0171200","WA0171300","WA0173600","WA0174100",
    "WA0174300","WA0175000","WA0180000","WA0270000","WA0270300","WA0272300","WA0310000","WA0310300",
    "WA0310500","WA0320000","WA0320400","WA0321500","WA0340000","WA0340100","WA0340400","WA0370100",
    "WA0390500","WASPD0000","WI0050000","WI0050200","WI0130100","WI0180100","WI0300100","WI0320100",
    "WI0411600","WI0450100","WI0520200","WI0540200","WI0680000","WI0680500","WI0710300","WIMPD0000",
    "WV0020000","WY0110100","WY0130100",
]

# ─── Async scraper ───────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_DELAY = 2.0


async def fetch_one(session, sem, ori, offense, from_date, to_date, results, errors_list, progress):
    url = f"{BASE}/summarized/agency/{ori}/{offense}"
    params = {"from": from_date, "to": to_date, "api_key": API_KEY}

    for attempt in range(MAX_RETRIES):
        async with sem:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 429:
                        wait = RETRY_DELAY * (2 ** attempt)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = await resp.json()

                    actuals = data.get("offenses", {}).get("actuals", {})
                    off_keys = [k for k in actuals if k.endswith("Offenses") and "United States" not in k]
                    clr_keys = [k for k in actuals if k.endswith("Clearances") and "United States" not in k]
                    off_key = off_keys[0] if off_keys else None
                    clr_key = clr_keys[0] if clr_keys else None

                    if off_key and ori not in results["names"]:
                        results["names"][ori] = off_key.replace(" Offenses", "")

                    if ori not in results["pops"]:
                        pop_dict = data.get("populations", {}).get("population", {})
                        pop_key = next((k for k in pop_dict
                                        if k != "United States" and k not in STATE_NAMES), None)
                        if pop_key:
                            pop_vals = list(pop_dict[pop_key].values())
                            if pop_vals:
                                results["pops"][ori] = pop_vals[0]

                    offense_data = actuals.get(off_key, {}) if off_key else {}
                    clearance_data = actuals.get(clr_key, {}) if clr_key else {}

                    for date_key, val in offense_data.items():
                        results["data"][(ori, offense, date_key)] = {
                            "actual": val,
                            "cleared": clearance_data.get(date_key),
                        }

                    progress["done"] += 1
                    if progress["done"] % 100 == 0 or progress["done"] == progress["total"]:
                        pct = progress["done"] / progress["total"] * 100
                        elapsed = time.time() - progress["start"]
                        rate = progress["done"] / elapsed if elapsed > 0 else 0
                        eta = (progress["total"] - progress["done"]) / rate if rate > 0 else 0
                        print(f"\r  {progress['done']:,}/{progress['total']:,} ({pct:.0f}%) "
                              f"- {rate:.0f} req/s, ETA {eta:.0f}s", end="", flush=True)
                    return

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES - 1:
                    errors_list.append({"ori": ori, "offense": offense, "error": str(e)})
                    progress["done"] += 1
                else:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))


async def scrape_all(from_date, to_date, concurrency):
    results = {"data": {}, "names": {}, "pops": {}}
    errors_list = []
    total = len(ORI_LIST) * len(OFFENSE_TYPES)
    progress = {"done": 0, "total": total, "start": time.time()}

    print(f"  {len(ORI_LIST)} ORIs x {len(OFFENSE_TYPES)} offenses = {total:,} requests")
    print(f"  Concurrency: {concurrency}")
    print(f"  Date range: {from_date} to {to_date}")

    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for ori in ORI_LIST:
            for offense in OFFENSE_TYPES:
                tasks.append(fetch_one(session, sem, ori, offense, from_date, to_date,
                                       results, errors_list, progress))
        await asyncio.gather(*tasks)

    print()
    return results, errors_list


# ─── Output ──────────────────────────────────────────────────────────────────

# Map CDE offense slugs to pipeline-friendly column names
OFFENSE_COL_MAP = {
    "homicide":           "homicide",
    "rape":               "rape",
    "robbery":            "robbery",
    "aggravated-assault": "aggravated_assault",
    "burglary":           "burglary",
    "larceny":            "larceny",
    "motor-vehicle-theft":"motor_vehicle_theft",
}


def build_csv(results, output_file):
    data = results["data"]
    names = results["names"]
    pops = results["pops"]

    all_dates = set()
    for (ori, offense, date_key) in data:
        all_dates.add(date_key)

    rows = []
    for ori in ORI_LIST:
        for date_key in sorted(all_dates):
            parts = date_key.split("-")
            month = parts[0] if len(parts) == 2 else ""
            year = parts[1] if len(parts) == 2 else date_key

            row = {
                "ori": ori,
                "agency_name": names.get(ori, ""),
                "state": ori[:2],
                "population": pops.get(ori, ""),
                "year": year,
                "month": month,
            }

            has_data = False
            for offense in OFFENSE_TYPES:
                col = OFFENSE_COL_MAP[offense]
                rec = data.get((ori, offense, date_key), {})
                row[f"{col}_actual"] = rec.get("actual", None)
                row[f"{col}_cleared"] = rec.get("cleared", None)
                if rec:
                    has_data = True

            if has_data:
                rows.append(row)

    rows.sort(key=lambda r: (r["state"], r["agency_name"], r["year"], r["month"]))

    base_cols = ["ori", "agency_name", "state", "population", "year", "month"]
    offense_cols = []
    for offense in OFFENSE_TYPES:
        col = OFFENSE_COL_MAP[offense]
        offense_cols.append(f"{col}_actual")
        offense_cols.append(f"{col}_cleared")
    fieldnames = base_cols + offense_cols

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return len(rows)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CDE SRS Agency Scraper")
    now = datetime.now()
    parser.add_argument("--from", dest="from_date", default="01-2017")
    parser.add_argument("--to", dest="to_date", default=f"{now.month:02d}-{now.year}")
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--output", default="cde_data.csv")
    args = parser.parse_args()

    print(f"FBI CDE SRS Scraper")
    print(f"{'='*50}")

    start = time.time()
    results, errors_list = asyncio.run(scrape_all(args.from_date, args.to_date, args.concurrency))
    elapsed_scrape = time.time() - start

    print(f"\n  Scraping done in {elapsed_scrape:.1f}s ({len(errors_list)} errors)")

    if errors_list:
        print(f"  First errors:")
        for e in errors_list[:10]:
            print(f"    {e['ori']} / {e['offense']}: {e['error']}")
        if len(errors_list) > 10:
            print(f"    ... and {len(errors_list) - 10} more")

    print(f"\nBuilding CSV...")
    row_count = build_csv(results, args.output)
    print(f"  {row_count:,} rows written to {args.output}")

    total_elapsed = time.time() - start
    print(f"\nTotal time: {total_elapsed:.1f}s")


if __name__ == "__main__":
    main()

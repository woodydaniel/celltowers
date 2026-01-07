#!/usr/bin/env python3
"""
Quick Test Script - Fetches 2 tower records from each of 3 US carriers.

Total: 6 records (2 Verizon, 2 AT&T, 2 T-Mobile)

Exit codes:
  0 - Success (all 6 records retrieved)
  1 - Partial success or error
  2 - CAPTCHA required (cookies expired/invalid)
"""

import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from scraper.api_client import CellMapperClient, CaptchaRequiredError, RateLimitedError
from scraper.parser import TowerParser, get_provider_name

# Test area: Downtown Los Angeles (high tower density for all carriers)
TEST_BOUNDS = {
    "north": 34.06,
    "south": 34.03,
    "east": -118.23,
    "west": -118.27,
}

# Primary MCC/MNC codes for each carrier
CARRIERS = {
    "Verizon": {"mcc": 311, "mnc": 480},
    "AT&T": {"mcc": 310, "mnc": 410},
    "T-Mobile": {"mcc": 310, "mnc": 260},
}

RECORDS_PER_CARRIER = 2


async def test_scrape() -> int:
    """
    Run a minimal test scrape: 2 records per carrier.
    
    Returns:
        Exit code (0=success, 1=partial/error, 2=captcha required)
    """
    
    print("=" * 60)
    print("CellMapper Test Scrape")
    print(f"Target: {RECORDS_PER_CARRIER} records per carrier ({len(CARRIERS)} carriers)")
    print(f"Total expected: {RECORDS_PER_CARRIER * len(CARRIERS)} records")
    print("=" * 60)
    print(f"\nTest Area: Downtown Los Angeles")
    print(f"Bounds: N={TEST_BOUNDS['north']}, S={TEST_BOUNDS['south']}, "
          f"E={TEST_BOUNDS['east']}, W={TEST_BOUNDS['west']}")
    print()
    
    all_records = []
    parser = TowerParser()
    captcha_carriers = []
    success_carriers = []
    
    async with CellMapperClient() as client:
        for carrier_name, codes in CARRIERS.items():
            mcc, mnc = codes["mcc"], codes["mnc"]
            provider = get_provider_name(mcc, mnc)
            
            print(f"\n{'─' * 50}")
            print(f"Fetching {carrier_name} (MCC={mcc}, MNC={mnc})...")
            
            try:
                response = await client.get_towers(
                    mcc=mcc,
                    mnc=mnc,
                    bounds=TEST_BOUNDS,
                    technology="LTE",
                )
                
                if response.success and response.data:
                    records, has_more = parser.parse_towers_response(
                        response.data, mcc, mnc, "LTE"
                    )
                    
                    # Take only the first N records
                    limited_records = records[:RECORDS_PER_CARRIER]
                    all_records.extend(limited_records)
                    
                    if len(records) > 0:
                        success_carriers.append(carrier_name)
                        print(f"✓ Found {len(records)} towers, keeping first {len(limited_records)}")
                        
                        for i, rec in enumerate(limited_records, 1):
                            bands_str = ", ".join(f"B{b}" for b in rec.bands[:4])
                            if len(rec.bands) > 4:
                                bands_str += f", +{len(rec.bands) - 4} more"
                            
                            print(f"  [{i}] Site {rec.site_id}: "
                                  f"({rec.latitude:.5f}, {rec.longitude:.5f}) "
                                  f"| Bands: {bands_str}")
                    else:
                        # Got response but no towers - likely CAPTCHA returned by API
                        # (the API client raises CaptchaRequiredError, but if it slips through
                        # the parser returns 0 records with status != OKAY)
                        captcha_carriers.append(carrier_name)
                        print(f"✗ No towers returned - likely CAPTCHA issue for {carrier_name}")
                        print(f"  Tip: Browse {carrier_name} towers on cellmapper.net first")
                else:
                    print(f"✗ Failed: {response.error or 'No data returned'}")
                    if response.raw_text:
                        preview = response.raw_text[:200]
                        print(f"  Response preview: {preview}...")
                        
            except CaptchaRequiredError as e:
                captcha_carriers.append(carrier_name)
                print(f"✗ CAPTCHA Required for {carrier_name}")
                print(f"  Error: {e}")
                print(f"\n  ⚠️  Your cookies don't work for {carrier_name}.")
                print(f"  To fix: Browse {carrier_name} towers on cellmapper.net,")
                print(f"  then extract fresh cookies.")
                
            except RateLimitedError as e:
                print(f"✗ Rate limited: {e}")
                print(f"  Try again in a few minutes.")
                
            except Exception as e:
                print(f"✗ Unexpected error: {type(e).__name__}: {e}")
    
    # Results summary
    print(f"\n{'=' * 60}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total records collected: {len(all_records)}")
    print(f"Successful carriers: {', '.join(success_carriers) or 'None'}")
    print(f"CAPTCHA issues: {', '.join(captcha_carriers) or 'None'}")
    print(f"API stats: {client.get_stats()}")
    
    # Determine exit code
    if len(all_records) == RECORDS_PER_CARRIER * len(CARRIERS):
        exit_code = 0
        status = "SUCCESS"
    elif captcha_carriers:
        exit_code = 2
        status = "CAPTCHA_REQUIRED"
    else:
        exit_code = 1
        status = "PARTIAL"
    
    print(f"\nStatus: {status} (exit code: {exit_code})")
    
    # Save results
    output_file = Path("data/test_results.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    results = {
        "test_bounds": TEST_BOUNDS,
        "records_per_carrier": RECORDS_PER_CARRIER,
        "total_records": len(all_records),
        "success_carriers": success_carriers,
        "captcha_carriers": captcha_carriers,
        "exit_code": exit_code,
        "towers": [rec.to_dict() for rec in all_records],
    }
    
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {output_file}")
    
    # Show collected data if any
    if all_records:
        print(f"\n{'─' * 60}")
        print("COLLECTED TOWER DATA:")
        print(f"{'─' * 60}")
        
        for rec in all_records:
            print(f"\n{rec.provider} - Site {rec.site_id}")
            print(f"  Location: ({rec.latitude}, {rec.longitude})")
            print(f"  Technology: {rec.technology}")
            print(f"  Bands: {rec.bands}")
            print(f"  Channels: {rec.channels[:5]}{'...' if len(rec.channels) > 5 else ''}")
    
    # Show help for CAPTCHA issues
    if captcha_carriers:
        print(f"\n{'=' * 60}")
        print("HOW TO FIX CAPTCHA ISSUES")
        print(f"{'=' * 60}")
        print(f"\nCarriers with CAPTCHA issues: {', '.join(captcha_carriers)}")
        print("\nSteps to fix:")
        print("1. Open https://www.cellmapper.net in your browser")
        print(f"2. Select '{captcha_carriers[0]}' from the carrier dropdown")
        print("3. Pan/zoom the map to load tower data")
        print("4. Open DevTools (F12) → Network tab")
        print("5. Look for 'getTowers' requests")
        print("6. Copy the Cookie header value")
        print("7. Run: CELLMAPPER_COOKIES='your_cookies_here' python test_run.py")
    
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(test_scrape())
    sys.exit(exit_code)

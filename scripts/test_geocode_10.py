#!/usr/bin/env python3
"""
Quick test script to reverse geocode 10 tower records using Smarty.com API
Adds columns: address, city, state, zip
"""

import json
import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SMARTY_AUTH_ID = os.getenv("SMARTY_AUTH_ID")
SMARTY_AUTH_TOKEN = os.getenv("SMARTY_AUTH_TOKEN")
SMARTY_API_URL = "https://us-reverse-geo.api.smarty.com/lookup"

# Input/Output files
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "downloads" / "test_sample_10_metro.jsonl"
OUTPUT_FILE = BASE_DIR / "downloads" / "test_sample_10_geocoded.jsonl"

def validate_credentials():
    """Check if Smarty credentials are configured"""
    if not SMARTY_AUTH_ID or not SMARTY_AUTH_TOKEN:
        print("ERROR: Smarty.com credentials not found!")
        print("Please set SMARTY_AUTH_ID and SMARTY_AUTH_TOKEN in .env file")
        sys.exit(1)
    print(f"✓ Credentials loaded")
    print(f"  Auth ID: {SMARTY_AUTH_ID[:8]}...")
    print(f"  Auth Token: {SMARTY_AUTH_TOKEN[:8]}...")

def is_valid_us_coordinate(lat, lon):
    """Check if coordinates are within valid US bounds"""
    # Continental US bounds (approximate)
    return (24 <= lat <= 50) and (-125 <= lon <= -66)

def reverse_geocode(latitude, longitude):
    """
    Call Smarty.com reverse geocoding API
    Returns dict with address, city, state, zip or None on error
    """
    try:
        # Build request
        params = {
            'auth-id': SMARTY_AUTH_ID,
            'auth-token': SMARTY_AUTH_TOKEN,
            'latitude': latitude,
            'longitude': longitude
        }
        
        response = requests.get(SMARTY_API_URL, params=params, timeout=10)
        
        # Check for errors
        if response.status_code == 401:
            print(f"  ERROR: Authentication failed (401)")
            return None
        elif response.status_code == 402:
            print(f"  ERROR: Payment required - check account quota (402)")
            return None
        elif response.status_code != 200:
            print(f"  ERROR: API returned status {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            return None
        
        # Parse response
        data = response.json()
        
        # Smarty returns {"results": [...]}
        results = data.get('results', [])
        if not results or len(results) == 0:
            print(f"  No address found for coordinates")
            return {
                'address': '',
                'city': '',
                'state': '',
                'zip': ''
            }
        
        # Get first (closest) result
        result = results[0]
        address_data = result.get('address', {})
        
        # Extract address components
        street = address_data.get('street', '')
        city = address_data.get('city', '')
        state = address_data.get('state_abbreviation', '')
        zipcode = address_data.get('zipcode', '')
        
        return {
            'address': street,
            'city': city,
            'state': state,
            'zip': zipcode
        }
        
    except requests.exceptions.Timeout:
        print(f"  ERROR: Request timeout")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: Request failed - {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"  ERROR: Failed to parse response - {e}")
        return None
    except Exception as e:
        print(f"  ERROR: Unexpected error - {e}")
        return None

def main():
    print("=" * 60)
    print("Smarty.com Reverse Geocoding Test (10 rows)")
    print("=" * 60)
    print()
    
    # Validate credentials
    validate_credentials()
    print()
    
    # Check input file
    if not INPUT_FILE.exists():
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()
    
    # Process towers
    processed = 0
    success = 0
    failed = 0
    invalid_coords = 0
    
    with open(INPUT_FILE, 'r') as infile, open(OUTPUT_FILE, 'w') as outfile:
        for line_num, line in enumerate(infile, 1):
            tower = json.loads(line.strip())
            
            lat = tower.get('latitude')
            lon = tower.get('longitude')
            tower_id = tower.get('tower_id', ['unknown'])[0]
            
            print(f"[{line_num}/10] Tower {tower_id}")
            print(f"  Coords: {lat}, {lon}")
            
            # Validate coordinates
            if not is_valid_us_coordinate(lat, lon):
                print(f"  ⚠ Invalid/non-US coordinates - skipping")
                tower['address'] = ''
                tower['city'] = ''
                tower['state'] = ''
                tower['zip'] = ''
                tower['geocode_status'] = 'invalid_coords'
                invalid_coords += 1
            else:
                # Call API
                result = reverse_geocode(lat, lon)
                
                if result:
                    tower['address'] = result['address']
                    tower['city'] = result['city']
                    tower['state'] = result['state']
                    tower['zip'] = result['zip']
                    tower['geocode_status'] = 'success'
                    
                    print(f"  ✓ {result['address']}")
                    print(f"    {result['city']}, {result['state']} {result['zip']}")
                    success += 1
                else:
                    tower['address'] = ''
                    tower['city'] = ''
                    tower['state'] = ''
                    tower['zip'] = ''
                    tower['geocode_status'] = 'failed'
                    failed += 1
            
            # Write result
            outfile.write(json.dumps(tower) + '\n')
            processed += 1
            print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total processed:     {processed}")
    print(f"Successfully geocoded: {success}")
    print(f"Failed:              {failed}")
    print(f"Invalid coordinates: {invalid_coords}")
    print()
    print(f"Output saved to: {OUTPUT_FILE}")
    print()
    
    # Show a sample result
    if success > 0:
        print("Sample geocoded record:")
        print("-" * 60)
        with open(OUTPUT_FILE, 'r') as f:
            for line in f:
                tower = json.loads(line)
                if tower.get('geocode_status') == 'success':
                    print(json.dumps({
                        'tower_id': tower['tower_id'],
                        'latitude': tower['latitude'],
                        'longitude': tower['longitude'],
                        'address': tower['address'],
                        'city': tower['city'],
                        'state': tower['state'],
                        'zip': tower['zip'],
                        'provider': tower['provider']
                    }, indent=2))
                    break

if __name__ == '__main__':
    main()

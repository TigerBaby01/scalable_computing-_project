#!/usr/bin/python3
import sys
import json
 
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        data = json.loads(line)
        route_id = data.get("route_id")
        # Fixed: Changed from delay_seconds to average_delay_seconds
        delay = data.get("average_delay_seconds")
        if route_id is not None and delay is not None:
            # Pass the route and its delay to the reducer
            print(f"{route_id}\t{delay}")
    except Exception:
        continue

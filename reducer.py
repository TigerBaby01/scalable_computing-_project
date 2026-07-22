#!/usr/bin/python3
import sys
 
current_route = None
current_sum = 0.0
current_count = 0
 
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    route_id, delay_str = line.split('\t', 1)
    try:
        # Fixed: Changed from int() to float() to handle decimals properly
        delay = float(delay_str)
    except ValueError:
        continue
 
    if current_route == route_id:
        current_sum += delay
        current_count += 1
    else:
        if current_route:
            avg_delay = round(current_sum / current_count, 2)
            print(f"{current_route}\t{avg_delay}")
        current_route = route_id
        current_sum = delay
        current_count = 1
 
if current_route:
    avg_delay = round(current_sum / current_count, 2)
    print(f"{current_route}\t{avg_delay}")

#!/usr/bin/env bash
#
# dump_hlcv_ist.sh
#
# Dumps every OHLCV sample for each stock, converting ts→IST, in one table per symbol.

# 1) Scan all metadata keys
metadata_keys=$(redis-cli --scan --pattern "stock:*:metadata")

if [[ -z "$metadata_keys" ]]; then
  echo "No stock data found in Redis."
  exit 1
fi

for metadata_key in $metadata_keys; do
  clean_symbol=${metadata_key#stock:}
  clean_symbol=${clean_symbol%:metadata}

  # Get human-readable name
  symbol_name=$(redis-cli HGET "$metadata_key" symbol_name)

  echo
  echo "============================================================================="
  echo "Stock: $symbol_name ($clean_symbol)"
  echo "============================================================================="
  printf "%-23s | %15s | %8s | %8s | %8s | %8s | %8s
" \
         "IST Timestamp" "Unix(ms)" "OPEN" "HIGH" "LOW" "CLOSE" "VOLUME"
  echo "------------------------------------------------------------------------------------------"

  # Stream ts/value pairs from the CLOSE series
  redis-cli TS.RANGE "stock:$clean_symbol:close" - + | \
  while read -r ts && read -r close_val; do
    # Round‑trip lookup at exact ts for the other metrics
    open_val=$(redis-cli TS.RANGE "stock:$clean_symbol:open"  "$ts" "$ts" | awk 'NR==2')
    high_val=$(redis-cli TS.RANGE "stock:$clean_symbol:high"  "$ts" "$ts" | awk 'NR==2')
    low_val=$(redis-cli TS.RANGE "stock:$clean_symbol:low"   "$ts" "$ts" | awk 'NR==2')
    volume_val=$(redis-cli TS.RANGE "stock:$clean_symbol:volume" "$ts" "$ts" | awk 'NR==2')

    # Convert to IST human time
    ist_time=$(TZ='Asia/Kolkata' date -d "@$((ts/1000))" "+%Y-%m-%d %H:%M:%S")

    # Print row
    printf "%-23s | %15s | %8s | %8s | %8s | %8s | %8s
" \
           "$ist_time" "$ts" "$open_val" "$high_val" "$low_val" "$close_val" "$volume_val"
  done
done

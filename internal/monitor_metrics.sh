#!/bin/bash

source "$(dirname "$0")/e2e_common.sh"

get_pg_metrics() {
  PGPASSWORD=$TARGET_PG_PASSWORD psql -h "$TARGET_PG_HOST" -U "$TARGET_PG_USER" -d "$TARGET_PG_DB" -p "$TARGET_PG_PORT" -t -c "
    SELECT
      (SELECT COUNT(*) FROM pg_stat_activity) AS active_connections,
      SUM(xact_commit) AS transactions_committed,
      SUM(xact_rollback) AS transactions_rolled_back,
      SUM(tup_inserted) AS rows_inserted,
      SUM(tup_updated) AS rows_updated,
      SUM(tup_deleted) AS rows_deleted
    FROM pg_stat_database;
  " | tr -d '|' | tr -s ' ' | sed 's/^ *//'
}

calculate_rates() {
  local current_values=($1)
  local previous_values=($2)
  local time_diff=$3

  for i in {1..5}; do
    rate=$(echo "scale=2; (${current_values[$i]} - ${previous_values[$i]}) / $time_diff" | bc 2>/dev/null)
    echo "${rate:-0}"
  done
}

monitor_pg_metrics() {
  local previous_values=($(get_pg_metrics))
  local start_time=$(date +%s.%N)

  while true; do
    sleep 1

    clear
    echo "PostgreSQL Metrics"
    echo "=================="

    local current_values=($(get_pg_metrics))
    local current_time=$(date +%s.%N)
    local time_diff=$(echo "$current_time - $start_time" | bc)

    local rates=($(calculate_rates "${current_values[*]}" "${previous_values[*]}" "$time_diff"))

    echo "Active Connections: ${current_values[0]:-N/A}"
    echo "Transactions Committed/sec: ${rates[0]:-0}"
    echo "Transactions Rolled Back/sec: ${rates[1]:-0}"
    echo "Insert Rate: ${rates[2]:-0} rows/sec"
    echo "Update Rate: ${rates[3]:-0} rows/sec"
    echo "Delete Rate: ${rates[4]:-0} rows/sec"

    previous_values=("${current_values[@]}")
    start_time=$current_time
  done
}

main() {
  if ! command -v tmux &>/dev/null; then
    echo "Error: tmux is not installed. Please install tmux and try again."
    exit 1
  fi

  monitor_pg_metrics
}

main

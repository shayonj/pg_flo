#!/usr/bin/env ruby

require 'pg'
require 'parallel'

def log(message)
  puts "📝 #{message}"
end

def success(message)
  puts "✅ #{message}"
end

def run_sql(sql)
  conn = PG.connect(
    host: ENV['PG_HOST'] || 'localhost',
    port: ENV['PG_PORT'] || 5433,
    dbname: ENV['PG_DB'] || 'mydb',
    user: ENV['PG_USER'] || 'myuser',
    password: ENV['PG_PASSWORD'] || 'mypassword'
  )
  conn.exec(sql)
ensure
  conn.close if conn
end

def setup_table
  log "Creating benchmark_test table in source database..."
  run_sql("CREATE TABLE IF NOT EXISTS benchmark_test (id SERIAL PRIMARY KEY, data TEXT);")
  success "Table created successfully"
end

def insert_rows(num_rows)
  batch_size = 1000
  num_processes = 8

  log "Inserting #{num_rows} rows into benchmark_test table..."

  total_inserted = Parallel.map(1..num_processes, in_processes: num_processes) do |process|
    conn = PG.connect(
      host: ENV['PG_HOST'] || 'localhost',
      port: ENV['PG_PORT'] || 5433,
      dbname: ENV['PG_DB'] || 'mydb',
      user: ENV['PG_USER'] || 'myuser',
      password: ENV['PG_PASSWORD'] || 'mypassword'
    )

    process_inserted = 0
    while process_inserted < num_rows / num_processes
      to_insert = [batch_size, num_rows / num_processes - process_inserted].min
      conn.exec("INSERT INTO benchmark_test (data) SELECT md5(random()::text) FROM generate_series(1, #{to_insert});")
      process_inserted += to_insert
      print "\rProcess #{process}: Inserted #{process_inserted} rows"
    end
    conn.close
    process_inserted
  end.sum

  puts "\n"
  success "Inserted #{total_inserted} rows successfully"
end

def main
  setup_table
  insert_rows(1_000_000)
end

main

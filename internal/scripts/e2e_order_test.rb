#!/usr/bin/env ruby

# This test verifies that pg_flo maintains transaction ordering and data consistency
# during concurrent operations. It creates parent-child relationships within transactions
# and runs multiple concurrent processes to ensure that:
# 1. Transaction atomicity is preserved
# 2. Parent-child relationships are maintained
# 3. Updates within transactions are ordered correctly
# 4. Concurrent modifications are handled properly

require 'pg'
require 'logger'
require 'securerandom'
require 'json'

class OrderTest
  # Test configuration
  TOTAL_OPERATIONS = 1000
  NUM_PROCESSES = 4
  OPERATIONS_PER_PROCESS = TOTAL_OPERATIONS / NUM_PROCESSES

  # Database configuration
  PG_HOST = 'localhost'
  PG_PORT = 5433
  PG_USER = 'myuser'
  PG_PASSWORD = 'mypassword!@#%1234'
  PG_DB = 'mydb'

  TARGET_PG_HOST = 'localhost'
  TARGET_PG_PORT = 5434
  TARGET_PG_USER = 'targetuser'
  TARGET_PG_PASSWORD = 'targetpassword!@#1234'
  TARGET_PG_DB = 'targetdb'

  # NATS configuration
  NATS_URL = 'nats://localhost:4222'

  # Paths
  PG_FLO_BIN = './bin/pg_flo'
  OUTPUT_DIR = '/tmp/pg_flo-output'
  PG_FLO_LOG = '/tmp/pg_flo.log'
  PG_FLO_WORKER_LOG = '/tmp/pg_flo_worker.log'

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc { |_, _, _, msg| "#{msg}\n" }

    sleep 5
    connect_to_databases
  end

  def connect_to_databases
    @source_db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    @target_db = PG.connect(
      host: TARGET_PG_HOST,
      port: TARGET_PG_PORT,
      dbname: TARGET_PG_DB,
      user: TARGET_PG_USER,
      password: TARGET_PG_PASSWORD
    )
  end


  def start_pg_flo
    @logger.info "Starting pg_flo..."

    replicator_cmd = "#{PG_FLO_BIN} replicator " \
      "--host #{PG_HOST} " \
      "--port #{PG_PORT} " \
      "--dbname #{PG_DB} " \
      "--user #{PG_USER} " \
      "--password #{PG_PASSWORD} " \
      "--group group_order " \
      "--tables order_test " \
      "--schema public " \
      "--nats-url #{NATS_URL}"

    worker_cmd = "#{PG_FLO_BIN} worker postgres " \
      "--group group_order " \
      "--nats-url #{NATS_URL} " \
      "--source-host #{PG_HOST} " \
      "--source-port #{PG_PORT} " \
      "--source-dbname #{PG_DB} " \
      "--source-user #{PG_USER} " \
      "--source-password #{PG_PASSWORD} " \
      "--target-host #{TARGET_PG_HOST} " \
      "--target-port #{TARGET_PG_PORT} " \
      "--target-dbname #{TARGET_PG_DB} " \
      "--target-user #{TARGET_PG_USER} " \
      "--target-password #{TARGET_PG_PASSWORD} " \
      "--target-sync-schema"

    @replicator_pid = spawn(replicator_cmd, out: PG_FLO_LOG, err: PG_FLO_LOG)
    sleep 2
    @worker_pid = spawn(worker_cmd, out: PG_FLO_WORKER_LOG, err: PG_FLO_WORKER_LOG)
    sleep 2
  end

  def run_concurrent_operations(process_id)
    # Create new connection for this process
    db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    @logger.info "Starting process #{process_id}..."
    existing_ids = []

    OPERATIONS_PER_PROCESS.times do |i|
      op_type = if existing_ids.empty?
        'INSERT'
      else
        rand(10) < 6 ? 'INSERT' : 'UPDATE'
      end

      case op_type
      when 'INSERT'
        result = db.exec_params(
          "INSERT INTO public.order_test (operation_type, process_id, sequence_num, data) " \
          "VALUES ($1, $2, $3, $4) RETURNING id",
          ['INSERT', process_id, i, "Data from process #{process_id}, seq #{i}"]
        )
        existing_ids << result[0]['id'].to_i if result.ntuples > 0
      when 'UPDATE'
        next if existing_ids.empty?
        update_id = existing_ids.sample
        db.exec_params(
          "UPDATE public.order_test SET data = $1 " \
          "WHERE id = $2",
          ["Updated: Process #{process_id}, seq #{i}", update_id]
        )
      end

      sleep(rand * 0.01) # Random delay up to 10ms
    end
  rescue => e
    @logger.error "Process #{process_id} error: #{e.message}"
    raise
  ensure
    db&.close
  end

  def run_update_storm(process_id)
    # Create new connection for this process
    db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    @logger.info "Starting update storm for process #{process_id}..."
    end_time = Time.now + 10 # Run for 10 seconds

    while Time.now < end_time
      result = db.exec_params(
        "SELECT id FROM public.order_test WHERE process_id = $1 ORDER BY random() LIMIT 1",
        [process_id]
      )

      unless result.ntuples.zero?
        row_id = result[0]['id']
        db.exec_params(
          "UPDATE public.order_test SET data = $1 WHERE id = $2",
          ["Storm update at #{Time.now.to_f}", row_id]
        )
      end

      sleep(rand * 0.005) # Random delay up to 5ms
    end
  rescue => e
    @logger.error "Storm process #{process_id} error: #{e.message}"
    raise
  ensure
    db&.close
  end

  def verify_order
    # Create new connections for verification
    source_db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    target_db = PG.connect(
      host: TARGET_PG_HOST,
      port: TARGET_PG_PORT,
      dbname: TARGET_PG_DB,
      user: TARGET_PG_USER,
      password: TARGET_PG_PASSWORD
    )

    @logger.info "Verifying operation order..."

    source_results = source_db.exec(
      "SELECT id, operation_type, process_id, sequence_num, data, version, created_at, updated_at " \
      "FROM public.order_test ORDER BY id"
    )

    target_results = target_db.exec(
      "SELECT id, operation_type, process_id, sequence_num, data, version, created_at, updated_at " \
      "FROM public.order_test ORDER BY id"
    )

    if source_results.ntuples != target_results.ntuples
      @logger.error "Row count mismatch! Source: #{source_results.ntuples}, Target: #{target_results.ntuples}"
      return false
    end

    source_results.each_with_index do |srow, i|
      trow = target_results[i]

      # Compare all fields
      if srow != trow
        @logger.error "Mismatch at row #{i + 1}:"
        @logger.error "Source: #{srow.inspect}"
        @logger.error "Target: #{trow.inspect}"
        return false
      end

      # Verify version numbers match (indicates update order preserved)
      if srow['version'] != trow['version']
        @logger.error "Version mismatch at row #{i + 1}:"
        @logger.error "Source version: #{srow['version']}, Target version: #{trow['version']}"
        return false
      end
    end

    @logger.info "Order verification passed! All operations replicated in correct order"
    true
  rescue => e
    @logger.error "Verification error: #{e.message}"
    false
  ensure
    source_db&.close
    target_db&.close
  end

  def cleanup
    Process.kill('TERM', @replicator_pid) if @replicator_pid
    Process.kill('TERM', @worker_pid) if @worker_pid
    Process.wait(@replicator_pid) if @replicator_pid
    Process.wait(@worker_pid) if @worker_pid
  end

  def create_test_table
    @logger.info "Creating test tables..."
    @source_db.exec(<<-SQL)
      DROP TABLE IF EXISTS public.order_test CASCADE;
      CREATE TABLE public.order_test (
        id serial PRIMARY KEY,
        operation_type text,
        process_id int,
        sequence_num int,
        data text,
        parent_id int,
        version int DEFAULT 1,
        created_at timestamp DEFAULT current_timestamp,
        updated_at timestamp DEFAULT current_timestamp,
        CONSTRAINT fk_parent
          FOREIGN KEY (parent_id)
          REFERENCES order_test(id)
          DEFERRABLE INITIALLY DEFERRED
      );

      CREATE OR REPLACE FUNCTION update_updated_at()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.updated_at = current_timestamp;
        NEW.version = OLD.version + 1;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER update_order_test_updated_at
        BEFORE UPDATE ON order_test
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at();
    SQL
    @logger.info "Test table created"
  end

  def drop_target_trigger
    @logger.info "Dropping trigger on target database..."
    @target_db.exec(<<-SQL)
      DROP TRIGGER IF EXISTS update_order_test_updated_at ON public.order_test;
      DROP FUNCTION IF EXISTS update_updated_at();
    SQL
    @logger.info "Target trigger dropped"
  end

  def run_transaction_test(process_id)
    db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    @logger.info "Starting transaction test for process #{process_id}..."

    begin
      # Start a transaction
      db.exec("BEGIN")

      sleep(rand * 2) # Random sleep up to 2s before parent insert

      # Insert parent record
      parent_result = db.exec_params(
        "INSERT INTO public.order_test (operation_type, process_id, sequence_num, data) " \
        "VALUES ($1, $2, $3, $4) RETURNING id",
        ['PARENT', process_id, 0, "Parent record from process #{process_id}"]
      )
      parent_id = parent_result[0]['id']

      # Insert multiple child records that reference the parent
      3.times do |i|
        sleep(rand * 0.05) # Random sleep up to 50ms between child inserts

        db.exec_params(
          "INSERT INTO public.order_test (operation_type, process_id, sequence_num, data, parent_id) " \
          "VALUES ($1, $2, $3, $4, $5)",
          ['CHILD', process_id, i + 1, "Child #{i + 1} of parent #{parent_id}", parent_id]
        )
      end

      sleep(rand * 0.1) # Random sleep up to 100ms before parent update

      # Update parent with child count
      db.exec_params(
        "UPDATE public.order_test SET data = $1 WHERE id = $2",
        ["Parent record with 3 children", parent_id]
      )

      sleep(rand * 0.2) # Random sleep up to 200ms before commit

      # Commit the transaction
      db.exec("COMMIT")
    rescue => e
      db.exec("ROLLBACK")
      raise e
    ensure
      db.close
    end
  end

  def verify_transaction_order
    source_db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    target_db = PG.connect(
      host: TARGET_PG_HOST,
      port: TARGET_PG_PORT,
      dbname: TARGET_PG_DB,
      user: TARGET_PG_USER,
      password: TARGET_PG_PASSWORD
    )

    @logger.info "Verifying transaction order..."

    begin
      # Check each parent and its children
      source_parents = source_db.exec(
        "SELECT * FROM public.order_test WHERE operation_type = 'PARENT' ORDER BY id"
      )

      target_parents = target_db.exec(
        "SELECT * FROM public.order_test WHERE operation_type = 'PARENT' ORDER BY id"
      )

      return false unless verify_result_sets(source_parents, target_parents, "Parents")

      source_parents.each do |parent|
        source_children = source_db.exec_params(
          "SELECT * FROM public.order_test WHERE parent_id = $1 ORDER BY sequence_num",
          [parent['id']]
        )

        target_children = target_db.exec_params(
          "SELECT * FROM public.order_test WHERE parent_id = $1 ORDER BY sequence_num",
          [parent['id']]
        )

        return false unless verify_result_sets(source_children, target_children, "Children of parent #{parent['id']}")
      end

      true
    ensure
      source_db.close
      target_db.close
    end
  end

  def verify_result_sets(source_results, target_results, context)
    if source_results.ntuples != target_results.ntuples
      @logger.error "#{context}: Count mismatch! Source: #{source_results.ntuples}, Target: #{target_results.ntuples}"
      return false
    end

    source_results.each_with_index do |srow, i|
      trow = target_results[i]

      if srow != trow
        @logger.error "#{context}: Mismatch at row #{i + 1}:"
        @logger.error "Source: #{srow.inspect}"
        @logger.error "Target: #{trow.inspect}"
        return false
      end

      if srow['version'] != trow['version']
        @logger.error "#{context}: Version mismatch at row #{i + 1}:"
        @logger.error "Source version: #{srow['version']}, Target version: #{trow['version']}"
        return false
      end
    end

    true
  end

  def run_test
    sleep 5  # Wait for Docker to be ready

    create_test_table
    start_pg_flo

    sleep 2  # Wait for initial replication setup
    drop_target_trigger

    # Run transaction tests
    transaction_processes = NUM_PROCESSES.times.map do |i|
      Process.fork { run_transaction_test(i + 1) }
    end

    transaction_processes.each { |pid| Process.wait(pid) }

    sleep 5

    success = verify_transaction_order
    cleanup


    exit(success ? 0 : 1)
  end
end

begin
  OrderTest.new.run_test
rescue => e
  puts "Error: #{e.message}"
  puts e.backtrace
  exit 1
end

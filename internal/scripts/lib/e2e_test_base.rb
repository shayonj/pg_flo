require 'pg'
require 'logger'
require 'securerandom'
require 'json'
require 'fileutils'

class E2ETestBase
  # Common configuration
  PG_HOST = 'localhost'
  PG_PORT = 5433
  PG_USER = 'myuser'
  PG_PASSWORD = 'mypassword'
  PG_DB = 'mydb'

  TARGET_PG_HOST = 'localhost'
  TARGET_PG_PORT = 5434
  TARGET_PG_USER = 'targetuser'
  TARGET_PG_PASSWORD = 'targetpassword'
  TARGET_PG_DB = 'targetdb'

  NATS_URL = 'nats://localhost:4222'
  PG_FLO_BIN = './bin/pg_flo'
  OUTPUT_DIR = '/tmp/pg_flo-output'
  PG_FLO_LOG = '/tmp/pg_flo.log'
  PG_FLO_WORKER_LOG = '/tmp/pg_flo_worker.log'

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc { |_, _, _, msg| "#{msg}\n" }
    @replicator_pids = []
    @worker_pids = []

    sleep 5 # Wait for Docker to be ready
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

  def start_pg_flo(group:, tables:, extra_args: {})
    replicator_cmd = build_replicator_cmd(group, tables, extra_args)
    worker_cmd = build_worker_cmd(group, extra_args)

    replicator_log = "#{PG_FLO_LOG}_#{group}"
    worker_log = "#{PG_FLO_WORKER_LOG}_#{group}"

    @logger.info "Starting pg_flo replicator for group #{group}..."
    replicator_pid = spawn(replicator_cmd, out: replicator_log, err: replicator_log)
    @replicator_pids << replicator_pid

    @logger.info "Starting pg_flo worker for group #{group}..."
    worker_pid = spawn(worker_cmd, out: worker_log, err: worker_log)
    @worker_pids << worker_pid

    sleep 2 # Wait for startup
  end

  def start_worker(group:, extra_args: {})
    @logger.info "Starting pg_flo worker for group #{group}..."
    worker_cmd = build_worker_cmd(group, extra_args)
    worker_log = "#{PG_FLO_WORKER_LOG}_#{group}"

    worker_pid = spawn(worker_cmd, out: worker_log, err: worker_log)
    @worker_pids << worker_pid

    sleep 2 # Wait for startup
  end

  def cleanup
    (@replicator_pids + @worker_pids).each do |pid|
      begin
        Process.kill('TERM', pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD
      end
    end
  end

  def run_test
    setup
    execute_test
    verify_results
  rescue => e
    @logger.error "Error during test: #{e.message}"
    @logger.error e.backtrace.join("\n")
    false
  ensure
    cleanup
  end

  def wait_for_replication(timeout: 30)
    @logger.info "Waiting for replication to complete..."
    timeout.times do |i|
      break if replication_complete?
      @logger.info "Waiting for replication (attempt #{i+1}/#{timeout})..."
      sleep 1
    end

    unless replication_complete?
      raise "Data not replicated after #{timeout} seconds"
    end
  end

  protected

  def replication_complete?
    raise NotImplementedError, "Subclasses must implement replication_complete?"
  end

  def assert_equals(actual, expected, message)
    unless actual.to_s == expected.to_s
      @logger.error "Assertion failed: #{message}"
      @logger.error "Expected: #{expected.inspect}"
      @logger.error "Actual: #{actual.inspect}"
      raise "Assertion failed: #{message}"
    end
    @logger.info "✓ #{message}"
  end

  def dump_table_states
    @logger.info "Source database state:"
    dump_table(@source_db, "users")
    dump_table(@source_db, "orders")
    dump_table(@source_db, "products")

    @logger.info "Target database state:"
    dump_table(@target_db, "customers")
    dump_table(@target_db, "transactions")
    dump_table(@target_db, "items")
  end

  private

  def build_replicator_cmd(group, tables, extra_args)
    cmd = "#{PG_FLO_BIN} replicator " \
      "--host #{PG_HOST} " \
      "--port #{PG_PORT} " \
      "--dbname #{PG_DB} " \
      "--user #{PG_USER} " \
      "--password #{PG_PASSWORD} " \
      "--group #{group} " \
      "--tables #{tables} " \
      "--schema public " \
      "--nats-url #{NATS_URL}"

    extra_args.each do |key, value|
      cmd += " --#{key} #{value}"
    end
    cmd
  end

  def build_worker_cmd(group, extra_args)
    cmd = "#{PG_FLO_BIN} worker postgres " \
      "--group #{group} " \
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

    extra_args.each do |key, value|
      cmd += " --#{key} #{value}"
    end
    cmd
  end

  def dump_table(db, table)
    @logger.info "Table: #{table}"
    result = db.exec("SELECT * FROM public.#{table}")
    result.each { |row| @logger.info row.inspect }
  rescue => e
    @logger.error "Failed to dump table #{table}: #{e.message}"
  end

  # These methods should be implemented by subclasses
  def setup
    raise NotImplementedError
  end

  def execute_test
    raise NotImplementedError
  end

  def verify_results
    raise NotImplementedError
  end
end

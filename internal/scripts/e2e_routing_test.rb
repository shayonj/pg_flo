#!/usr/bin/env ruby

require_relative 'lib/e2e_test_base'
require 'yaml'

class RoutingTest < E2ETestBase
  def setup
    create_tables
    create_routing_config
    create_rules_config
  end

  def execute_test
    start_pg_flo(
      group: 'routing_test',
      tables: 'users,orders,products'
    )

    sleep 2  # Wait for replicator to initialize

    start_worker(
      group: 'routing_test',
      extra_args: {
        'routing-config' => @routing_config_path,
        'rules-config' => @rules_config_path
      }
    )

    simulate_changes
    wait_for_replication
  end

  def verify_results
    verify_changes
  end

  private

  def create_tables
    @logger.info "Creating test tables in source and target databases..."

    # Source database tables
    @source_db.exec(<<-SQL)
      DROP TABLE IF EXISTS public.users CASCADE;
      DROP TABLE IF EXISTS public.orders CASCADE;
      DROP TABLE IF EXISTS public.products CASCADE;

      CREATE TABLE public.users (
        id bigserial PRIMARY KEY,
        username varchar(50) NOT NULL,
        email varchar(100) NOT NULL,
        subscription_type jsonb,
        metadata jsonb
      );

      CREATE TABLE public.orders (
        id bigserial PRIMARY KEY,
        user_id bigint REFERENCES public.users(id),
        total_amount numeric(15,2) NOT NULL,
        items jsonb
      );

      CREATE TABLE public.products (
        id bigserial PRIMARY KEY,
        name varchar(100) NOT NULL,
        stock jsonb,
        tags text[]
      );
    SQL

    # Target database tables
    @target_db.exec(<<-SQL)
      DROP TABLE IF EXISTS public.customers CASCADE;
      DROP TABLE IF EXISTS public.transactions CASCADE;
      DROP TABLE IF EXISTS public.items CASCADE;

      CREATE TABLE public.customers (
        customer_id bigserial PRIMARY KEY,
        customer_name varchar(50) NOT NULL,
        email varchar(100) NOT NULL,
        subscription_type jsonb,
        metadata jsonb
      );

      CREATE TABLE public.transactions (
        transaction_id bigserial PRIMARY KEY,
        user_id bigint,
        amount numeric(15,2) NOT NULL,
        items jsonb
      );

      CREATE TABLE public.items (
        item_id bigserial PRIMARY KEY,
        item_name varchar(100) NOT NULL,
        stock jsonb,
        tags text[]
      );
    SQL

    @logger.info "Test tables created in source and target databases"
  end

  def create_routing_config
    @routing_config_path = '/tmp/pg_flo_routing_test.yml'
    config = {
      'users' => {
        'source_table' => 'users',
        'destination_table' => 'customers',
        'column_mappings' => [
          { 'source' => 'id', 'destination' => 'customer_id' },
          { 'source' => 'username', 'destination' => 'customer_name' },
          { 'source' => 'email', 'destination' => 'email' },
          { 'source' => 'subscription_type', 'destination' => 'subscription_type' },
          { 'source' => 'metadata', 'destination' => 'metadata' }
        ],
        'operations' => ['INSERT', 'UPDATE']
      },
      'orders' => {
        'source_table' => 'orders',
        'destination_table' => 'transactions',
        'column_mappings' => [
          { 'source' => 'id', 'destination' => 'transaction_id' },
          { 'source' => 'total_amount', 'destination' => 'amount' },
          { 'source' => 'items', 'destination' => 'items' }
        ],
        'operations' => ['INSERT', 'UPDATE', 'DELETE']
      },
      'products' => {
        'source_table' => 'products',
        'destination_table' => 'items',
        'column_mappings' => [
          { 'source' => 'id', 'destination' => 'item_id' },
          { 'source' => 'name', 'destination' => 'item_name' },
          { 'source' => 'stock', 'destination' => 'stock' },
          { 'source' => 'tags', 'destination' => 'tags' }
        ],
        'operations' => ['INSERT', 'UPDATE']
      }
    }

    File.write(@routing_config_path, config.to_yaml)
    @logger.info "Routing configuration created"
  end

  def create_rules_config
    @rules_config_path = '/tmp/pg_flo_rules_test.yml'
    config = {
      'users' => [
        {
          'type' => 'transform',
          'column' => 'email',
          'parameters' => {
            'operation' => 'lowercase'
          }
        },
        {
          'type' => 'transform',
          'column' => 'metadata',
          'parameters' => {
            'operation' => 'jsonb_extract_path',
            'path' => 'subscription_type'
          }
        }
      ],
      'orders' => [
        {
          'type' => 'filter',
          'column' => 'total_amount',
          'parameters' => {
            'operator' => 'gt',
            'value' => 100
          }
        },
        {
          'type' => 'transform',
          'column' => 'items',
          'parameters' => {
            'operation' => 'jsonb_array_length'
          }
        }
      ],
      'products' => [
        {
          'type' => 'transform',
          'column' => 'inventory',
          'parameters' => {
            'operation' => 'jsonb_extract_path',
            'path' => 'stock'
          }
        },
        {
          'type' => 'transform',
          'column' => 'tags',
          'parameters' => {
            'operation' => 'array_length'
          }
        }
      ]
    }

    File.write(@rules_config_path, config.to_yaml)
    @logger.info "Rules configuration created"
  end

  def simulate_changes
    @logger.info "Simulating changes..."

    @source_db.exec(<<-SQL)
      INSERT INTO public.users (username, email, subscription_type)
      VALUES ('john_doe', 'updated@example.com', '{"type": "enterprise"}');

      INSERT INTO public.users (username, email)
      VALUES ('jane_smith', 'jane@example.com');

      INSERT INTO public.orders (user_id, total_amount, items)
      VALUES (1, 50.00, '[{"id": 1}, {"id": 2}]');

      INSERT INTO public.orders (user_id, total_amount, items)
      VALUES (1, 150.00, '[{"id": 3}, {"id": 4}]');

      INSERT INTO public.products (name, stock, tags)
      VALUES ('Widget', '{"quantity": 75}', '{"electronic","gadget","premium"}');

      INSERT INTO public.products (name, stock, tags)
      VALUES ('Gadget', '{"quantity": 50}', '{"tool","premium"}');
    SQL

    @logger.info "Changes simulated"
  end

  def verify_changes
    @logger.info "Verifying changes in target database..."

    wait_for_replication

    results = fetch_verification_results
    verify_results_data(results)
  rescue => e
    @logger.error "Verification failed: #{e.message}"
    dump_table_states
    raise
  end

  private

  def data_replicated?
    user_count = @target_db.exec("SELECT COUNT(*) FROM public.customers").getvalue(0,0).to_i
    user_count == 2
  end

  def fetch_verification_results
    {}.tap do |results|
      # Users verification
      users_result = @target_db.exec(<<-SQL)
        SELECT
          COUNT(*) as user_count,
          (SELECT email FROM public.customers WHERE customer_name = 'john_doe') as john_email,
          (SELECT email FROM public.customers WHERE customer_name = 'jane_smith') as jane_email,
          (SELECT subscription_type->>'type' FROM public.customers WHERE customer_name = 'john_doe') as john_subscription
        FROM public.customers
      SQL

      results[:user_count] = users_result.getvalue(0, 0)
      results[:john_email] = users_result.getvalue(0, 1)
      results[:jane_email] = users_result.getvalue(0, 2)
      results[:john_subscription] = users_result.getvalue(0, 3)
    end
  end

  def verify_results_data(results)
    assert_equals(results[:user_count], "2", "Customer count")
    assert_equals(results[:john_email], "updated@example.com", "John's email")
    assert_equals(results[:jane_email], "jane@example.com", "Jane's email")
    assert_equals(results[:john_subscription], "enterprise", "John's subscription type")
    assert_equals(results[:order_count], "2", "Transaction count")
    assert_equals(results[:high_value_order], "150.00", "High-value order amount")
    assert_equals(results[:order_items_count], "2", "Order items count")
    assert_equals(results[:product_count], "2", "Item count")
    assert_equals(results[:widget_stock], "75", "Widget stock")
    assert_equals(results[:widget_tags_count], "3", "Widget tags count")
  end

  def assert_equals(actual, expected, message)
    unless actual.to_s == expected.to_s
      @logger.error "Assertion failed: #{message} - Expected '#{expected}', but got '#{actual}'"
      raise "Assertion failed: #{message}"
    end
    @logger.info "Assertion passed: #{message}"
  end

  protected

  def replication_complete?
    user_count = @target_db.exec("SELECT COUNT(*) FROM public.customers").getvalue(0,0).to_i
    user_count == 2
  end
end

# Run the test
if __FILE__ == $0
  test = RoutingTest.new
  success = test.run_test
  exit(success ? 0 : 1)
end

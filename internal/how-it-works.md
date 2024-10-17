# How it Works

`pg_flo` leverages PostgreSQL's logical replication system to capture and stream data while applying transformations and filtrations to the data before it reaches the destination. It utilizes **NATS** as a message broker to decouple the replicator and worker processes, providing flexibility and scalability.

1. **Publication Creation**: Creates a PostgreSQL publication for the specified tables or all tables (per `group`).

2. **Replication Slot**: A replication slot is created to ensure no data is lost between streaming sessions.

3. **Operation Modes**:

   - **Copy-and-Stream**: Performs an initial bulk copy followed by streaming changes.
   - **Stream-Only**: Starts streaming changes immediately from the last known position.

4. **Initial Bulk Copy** (for Copy-and-Stream mode):

   - If no valid LSN is found in NATS, `pg_flo` performs an initial bulk copy of existing data.
   - This process is parallelized for fast data sync:
     - A snapshot is taken to ensure consistency.
     - Each table is divided into page ranges.
     - Multiple workers copy different ranges concurrently.

5. **Streaming Changes**:

   - After the initial copy (or immediately in Stream-Only mode), the replicator streams changes from PostgreSQL and publishes them to NATS.
   - The last processed LSN is stored in NATS, allowing `pg_flo` to resume operations from where it left off in case of interruptions.

6. **Message Processing**: The worker processes various types of messages from NATS:

   - Relation messages to understand table structures
   - Insert, Update, and Delete messages containing actual data changes
   - Begin and Commit messages for transaction boundaries
   - DDL changes like ALTER TABLE, CREATE INDEX, etc.

7. **Data Transformation**: Received data is converted into a structured format, with type-aware conversions for different PostgreSQL data types.

8. **Rule Application**: If configured, transformation and filtering rules are applied to the data:

   - **Transform Rules**:
     - Regex: Apply regular expression transformations to string values.
     - Mask: Mask sensitive data, keeping the first and last characters visible.
   - **Filter Rules**:
     - Comparison: Filter based on equality, inequality, greater than, less than, etc.
     - Contains: Filter string values based on whether they contain a specific substring.
   - Rules can be applied selectively to insert, update, or delete operations.

9. **Buffering**: Processed data is buffered and written in batches to optimize write operations to the destination.

10. **Writing to Sink**: Data is periodically flushed from the buffer to the configured sink (e.g., stdout, file, or other destinations).

11. **State Management**:
    - The replicator keeps track of its progress by updating the Last LSN in NATS.
    - The worker maintains its progress to ensure data consistency.
    - This allows for resumable operations across multiple runs.
    - Periodic status updates are sent to PostgreSQL to maintain the replication connection.

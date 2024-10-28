package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/routing"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
	"github.com/shayonj/pg_flo/pkg/worker"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "pg_flo",
		Short: "The easiest way to move and transform data from PostgreSQL",
	}

	replicatorCmd = &cobra.Command{
		Use:   "replicator",
		Short: "Start the replicator",
		Long:  `Start the replicator to capture changes from PostgreSQL and publish to NATS`,
		Run:   runReplicator,
	}

	workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Start the worker",
		Long:  `Start the worker to process messages from NATS, apply rules, and write to sinks`,
	}

	stdoutWorkerCmd = &cobra.Command{
		Use:   "stdout",
		Short: "Start the worker with stdout sink",
		Run:   runWorker,
	}

	fileWorkerCmd = &cobra.Command{
		Use:   "file",
		Short: "Start the worker with file sink",
		Run:   runWorker,
	}

	postgresWorkerCmd = &cobra.Command{
		Use:   "postgres",
		Short: "Start the worker with postgres sink",
		Run:   runWorker,
	}

	webhookWorkerCmd = &cobra.Command{
		Use:   "webhook",
		Short: "Start the worker with webhook sink",
		Run:   runWorker,
	}

	replayCmd = &cobra.Command{
		Use:   "replay-worker",
		Short: "Replay historical changes",
		Run:   runReplay,
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pg_flo.yaml)")

	// Replicator flags
	replicatorCmd.Flags().String("host", "", "PostgreSQL host (env: PG_FLO_HOST)")
	replicatorCmd.Flags().Int("port", 5432, "PostgreSQL port (env: PG_FLO_PORT)")
	replicatorCmd.Flags().String("dbname", "", "PostgreSQL database name (env: PG_FLO_DBNAME)")
	replicatorCmd.Flags().String("user", "", "PostgreSQL user (env: PG_FLO_USER)")
	replicatorCmd.Flags().String("password", "", "PostgreSQL password (env: PG_FLO_PASSWORD)")
	replicatorCmd.Flags().String("group", "", "Group name for replication (env: PG_FLO_GROUP)")
	replicatorCmd.Flags().String("schema", "public", "PostgreSQL schema (env: PG_FLO_SCHEMA)")
	replicatorCmd.Flags().StringSlice("tables", []string{}, "Tables to replicate (env: PG_FLO_TABLES)")
	replicatorCmd.Flags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	replicatorCmd.Flags().Bool("copy-and-stream", false, "Enable copy and stream mode (env: PG_FLO_COPY_AND_STREAM)")
	replicatorCmd.Flags().Int("max-copy-workers-per-table", 4, "Maximum number of copy workers per table (env: PG_FLO_MAX_COPY_WORKERS_PER_TABLE)")
	replicatorCmd.Flags().Bool("track-ddl", false, "Enable tracking of DDL changes (env: PG_FLO_TRACK_DDL)")
	replicatorCmd.Flags().Duration("max-age", 24*time.Hour, "Maximum age of messages to retain (env: PG_FLO_MAX_AGE)")
	replicatorCmd.Flags().Bool("enable-replays", false, "Enable replay capability - requires more storage (env: PG_FLO_ENABLE_REPLAYS)")
	replicatorCmd.Flags().Int("nats-replicas", 1, "Number of stream replicas (env: PG_FLO_NATS_REPLICAS)")

	markFlagRequired(replicatorCmd, "host", "port", "dbname", "user", "password", "group", "nats-url")

	// Worker flags
	workerCmd.PersistentFlags().String("group", "", "Group name for worker (env: PG_FLO_GROUP)")
	workerCmd.PersistentFlags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	workerCmd.PersistentFlags().String("rules-config", "", "Path to rules configuration file (env: PG_FLO_RULES_CONFIG)")
	workerCmd.PersistentFlags().String("routing-config", "", "Path to routing configuration file (env: PG_FLO_ROUTING_CONFIG)")

	markPersistentFlagRequired(workerCmd, "group", "nats-url")

	// File sink flags
	fileWorkerCmd.Flags().String("file-output-dir", "/tmp", "Output directory for file sink (env: PG_FLO_FILE_OUTPUT_DIR)")

	// Postgres sink flags
	postgresWorkerCmd.Flags().String("target-host", "", "Target PostgreSQL host (env: PG_FLO_TARGET_HOST)")
	postgresWorkerCmd.Flags().Int("target-port", 5432, "Target PostgreSQL port (env: PG_FLO_TARGET_PORT)")
	postgresWorkerCmd.Flags().String("target-dbname", "", "Target PostgreSQL database name (env: PG_FLO_TARGET_DBNAME)")
	postgresWorkerCmd.Flags().String("target-user", "", "Target PostgreSQL user (env: PG_FLO_TARGET_USER)")
	postgresWorkerCmd.Flags().String("target-password", "", "Target PostgreSQL password (env: PG_FLO_TARGET_PASSWORD)")
	postgresWorkerCmd.Flags().Bool("target-sync-schema", false, "Sync schema from source to target (env: PG_FLO_TARGET_SYNC_SCHEMA)")
	postgresWorkerCmd.Flags().Bool("target-disable-foreign-keys", false, "Disable foreign key checks during write (env: PG_FLO_TARGET_DISABLE_FOREIGN_KEYS)")

	postgresWorkerCmd.Flags().String("source-host", "", "Source PostgreSQL host (env: PG_FLO_SOURCE_HOST)")
	postgresWorkerCmd.Flags().Int("source-port", 5432, "Source PostgreSQL port (env: PG_FLO_SOURCE_PORT)")
	postgresWorkerCmd.Flags().String("source-dbname", "", "Source PostgreSQL database name (env: PG_FLO_SOURCE_DBNAME)")
	postgresWorkerCmd.Flags().String("source-user", "", "Source PostgreSQL user (env: PG_FLO_SOURCE_USER)")
	postgresWorkerCmd.Flags().String("source-password", "", "Source PostgreSQL password (env: PG_FLO_SOURCE_PASSWORD)")

	markFlagRequired(postgresWorkerCmd, "target-host", "target-dbname", "target-user", "target-password")

	// Webhook sink flags
	webhookWorkerCmd.Flags().String("webhook-url", "", "Webhook URL to send data (env: PG_FLO_WEBHOOK_URL)")

	markFlagRequired(webhookWorkerCmd, "webhook-url")

	// Add subcommands to worker command
	workerCmd.AddCommand(stdoutWorkerCmd, fileWorkerCmd, postgresWorkerCmd, webhookWorkerCmd)

	// Add replay command
	replayCmd.Flags().String("start-time", "", "Start time for replay (RFC3339 format)")
	replayCmd.Flags().String("end-time", "", "End time for replay (RFC3339 format)")
	replayCmd.Flags().String("group", "", "Group name for worker (env: PG_FLO_GROUP)")
	replayCmd.Flags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	replayCmd.Flags().String("rules-config", "", "Path to rules configuration file (env: PG_FLO_RULES_CONFIG)")
	replayCmd.Flags().String("routing-config", "", "Path to routing configuration file (env: PG_FLO_ROUTING_CONFIG)")

	markFlagRequired(replayCmd, "start-time", "end-time", "group", "nats-url")

	rootCmd.AddCommand(replicatorCmd, workerCmd, replayCmd)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".pg_flo")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("PG_FLO")

	bindFlags(replicatorCmd)
	bindFlags(workerCmd)
	bindFlags(stdoutWorkerCmd)
	bindFlags(fileWorkerCmd)
	bindFlags(postgresWorkerCmd)
	bindFlags(webhookWorkerCmd)
	bindFlags(replayCmd)

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func bindFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindEnv(f.Name, fmt.Sprintf("PG_FLO_%s", strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_")))); err != nil {
			fmt.Printf("Error binding flag %s to env var: %v\n", f.Name, err)
		}
		if err := viper.BindPFlag(f.Name, f); err != nil {
			fmt.Printf("Error binding flag %s: %v\n", f.Name, err)
		}
	})
}

func runReplicator(_ *cobra.Command, _ []string) {
	config := replicator.Config{
		Host:     viper.GetString("host"),
		Port:     uint16(viper.GetInt("port")),
		Database: viper.GetString("dbname"),
		User:     viper.GetString("user"),
		Password: viper.GetString("password"),
		Group:    viper.GetString("group"),
		Schema:   viper.GetString("schema"),
		Tables:   viper.GetStringSlice("tables"),
		TrackDDL: viper.GetBool("track-ddl"),
	}

	natsURL := viper.GetString("nats-url")
	if natsURL == "" {
		log.Fatal().Msg("NATS URL is required")
	}

	natsConfig := pgflonats.StreamConfig{
		MaxAge:   viper.GetDuration("max-age"),
		Replays:  viper.GetBool("enable-replays"),
		Replicas: viper.GetInt("nats-replicas"),
	}

	natsClient, err := pgflonats.NewNATSClient(natsURL, fmt.Sprintf("pgflo_%s_stream", config.Group), config.Group, natsConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NATS client")
	}

	copyAndStream := viper.GetBool("copy-and-stream")
	maxCopyWorkersPerTable := viper.GetInt("max-copy-workers-per-table")

	rep, err := replicator.NewReplicator(config, natsClient, copyAndStream, maxCopyWorkersPerTable)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create replicator")
	}

	if err := rep.StartReplication(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start replication")
	}
}

func runWorker(cmd *cobra.Command, _ []string) {
	group := viper.GetString("group")
	natsURL := viper.GetString("nats-url")
	if natsURL == "" {
		log.Fatal().Msg("NATS URL is required")
	}

	rulesConfigPath := viper.GetString("rules-config")
	routingConfigPath := viper.GetString("routing-config")
	sinkType := cmd.Use

	// Create NATS client
	natsConfig := pgflonats.DefaultStreamConfig()
	natsClient, err := pgflonats.NewNATSClient(natsURL, fmt.Sprintf("pgflo_%s_stream", group), group, natsConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NATS client")
	}

	ruleEngine := rules.NewRuleEngine()
	if rulesConfigPath != "" {
		rulesConfig, err := loadRulesConfig(rulesConfigPath)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules configuration")
		}
		if err := ruleEngine.LoadRules(rulesConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules")
		}
	}

	router := routing.NewRouter()
	if routingConfigPath != "" {
		routingConfig, err := loadRoutingConfig(routingConfigPath)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load routing configuration")
		}
		if err := router.LoadRoutes(routingConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load routes")
		}
	}

	sink, err := createSink(sinkType)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sink")
	}

	w := worker.NewWorker(natsClient, ruleEngine, router, sink, group)

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	log.Info().Msg("Starting worker...")
	if err := w.Start(ctx); err != nil {
		if err == context.Canceled {
			log.Info().Msg("Worker shut down gracefully")
		} else {
			log.Error().Err(err).Msg("Worker encountered an error during shutdown")
		}
	}
}

func loadRulesConfig(filePath string) (rules.Config, error) {
	var config rules.Config
	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

func loadRoutingConfig(filePath string) (map[string]routing.TableRoute, error) {
	var config map[string]routing.TableRoute
	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read routing config file: %w", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal routing config: %w", err)
	}
	return config, nil
}

func createSink(sinkType string) (sinks.Sink, error) {
	switch sinkType {
	case "stdout":
		return sinks.NewStdoutSink()
	case "file":
		return sinks.NewFileSink(
			viper.GetString("file-output-dir"),
		)
	case "postgres":
		return sinks.NewPostgresSink(
			viper.GetString("target-host"),
			viper.GetInt("target-port"),
			viper.GetString("target-dbname"),
			viper.GetString("target-user"),
			viper.GetString("target-password"),
			viper.GetBool("target-sync-schema"),
			viper.GetString("source-host"),
			viper.GetInt("source-port"),
			viper.GetString("source-dbname"),
			viper.GetString("source-user"),
			viper.GetString("source-password"),
			viper.GetBool("target-disable-foreign-keys"),
		)
	case "webhook":
		return sinks.NewWebhookSink(
			viper.GetString("webhook-url"),
		)
	default:
		return nil, fmt.Errorf("unknown sink type: %s", sinkType)
	}
}

// Helper function to mark multiple flags as required
func markFlagRequired(cmd *cobra.Command, flags ...string) {
	for _, flag := range flags {
		if err := cmd.MarkFlagRequired(flag); err != nil {
			fmt.Printf("Error marking flag %s as required: %v\n", flag, err)
		}
	}
}

// Helper function to mark multiple persistent flags as required
func markPersistentFlagRequired(cmd *cobra.Command, flags ...string) {
	for _, flag := range flags {
		if err := cmd.MarkPersistentFlagRequired(flag); err != nil {
			fmt.Printf("Error marking persistent flag %s as required: %v\n", flag, err)
		}
	}
}

func runReplay(cmd *cobra.Command, _ []string) {
	startTimeStr := viper.GetString("start-time")
	endTimeStr := viper.GetString("end-time")

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid start time format")
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid end time format")
	}

	// Create NATS client with replay config
	natsConfig := pgflonats.StreamConfig{
		MaxAge:  viper.GetDuration("max-age"),
		Replays: viper.GetBool("enable-replays"),
	}

	natsClient, err := pgflonats.NewNATSClient(
		viper.GetString("nats-url"),
		fmt.Sprintf("pgflo_%s_stream", viper.GetString("group")),
		viper.GetString("group"),
		natsConfig,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NATS client")
	}

	ruleEngine := rules.NewRuleEngine()
	if viper.GetString("rules-config") != "" {
		rulesConfig, err := loadRulesConfig(viper.GetString("rules-config"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules configuration")
		}
		if err := ruleEngine.LoadRules(rulesConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules")
		}
	}

	router := routing.NewRouter()
	if viper.GetString("routing-config") != "" {
		routingConfig, err := loadRoutingConfig(viper.GetString("routing-config"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load routing configuration")
		}
		if err := router.LoadRoutes(routingConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load routes")
		}
	}

	sink, err := createSink(cmd.Use)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sink")
	}

	w := worker.NewWorker(natsClient, ruleEngine, router, sink, viper.GetString("group"))

	replayWorker := worker.NewReplayWorker(w, startTime, endTime)

	if err := replayWorker.Start(cmd.Context()); err != nil {
		log.Fatal().Err(err).Msg("Failed to start replay worker")
	}
}

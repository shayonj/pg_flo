package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/pgflonats"
	"github.com/shayonj/pg_flo/pkg/replicator"
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
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
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

	replicatorCmd.MarkFlagRequired("host")
	replicatorCmd.MarkFlagRequired("port")
	replicatorCmd.MarkFlagRequired("dbname")
	replicatorCmd.MarkFlagRequired("user")
	replicatorCmd.MarkFlagRequired("password")
	replicatorCmd.MarkFlagRequired("group")
	replicatorCmd.MarkFlagRequired("nats-url")

	// Worker flags
	workerCmd.PersistentFlags().String("group", "", "Group name for worker (env: PG_FLO_GROUP)")
	workerCmd.PersistentFlags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	workerCmd.PersistentFlags().String("rules-config", "", "Path to rules configuration file (env: PG_FLO_RULES_CONFIG)")

	workerCmd.MarkFlagRequired("group")
	workerCmd.MarkFlagRequired("nats-url")

	// Stdout sink flags
	stdoutWorkerCmd.Flags().String("stdout-format", "json", "Output format for stdout sink (json or csv) (env: PG_FLO_STDOUT_FORMAT)")

	// File sink flags
	fileWorkerCmd.Flags().String("file-output-dir", "/tmp", "Output directory for file sink (env: PG_FLO_FILE_OUTPUT_DIR)")

	// Postgres sink flags
	postgresWorkerCmd.Flags().String("postgres-host", "", "Target PostgreSQL host (env: PG_FLO_POSTGRES_HOST)")
	postgresWorkerCmd.Flags().Int("postgres-port", 5432, "Target PostgreSQL port (env: PG_FLO_POSTGRES_PORT)")
	postgresWorkerCmd.Flags().String("postgres-dbname", "", "Target PostgreSQL database name (env: PG_FLO_POSTGRES_DBNAME)")
	postgresWorkerCmd.Flags().String("postgres-user", "", "Target PostgreSQL user (env: PG_FLO_POSTGRES_USER)")
	postgresWorkerCmd.Flags().String("postgres-password", "", "Target PostgreSQL password (env: PG_FLO_POSTGRES_PASSWORD)")
	postgresWorkerCmd.Flags().Bool("postgres-sync-schema", false, "Sync schema from source to target (env: PG_FLO_POSTGRES_SYNC_SCHEMA)")

	postgresWorkerCmd.MarkFlagRequired("postgres-host")
	postgresWorkerCmd.MarkFlagRequired("postgres-dbname")
	postgresWorkerCmd.MarkFlagRequired("postgres-user")
	postgresWorkerCmd.MarkFlagRequired("postgres-password")

	// Webhook sink flags
	webhookWorkerCmd.Flags().String("webhook-url", "", "Webhook URL to send data (env: PG_FLO_WEBHOOK_URL)")
	webhookWorkerCmd.Flags().Int("webhook-batch-size", 100, "Number of messages to batch before sending (env: PG_FLO_WEBHOOK_BATCH_SIZE)")
	webhookWorkerCmd.Flags().Int("webhook-retry-max", 3, "Maximum number of retries for failed requests (env: PG_FLO_WEBHOOK_RETRY_MAX)")

	webhookWorkerCmd.MarkFlagRequired("webhook-url")

	// Add subcommands to worker command
	workerCmd.AddCommand(stdoutWorkerCmd, fileWorkerCmd, postgresWorkerCmd, webhookWorkerCmd)

	rootCmd.AddCommand(replicatorCmd, workerCmd)
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

func runReplicator(cmd *cobra.Command, args []string) {
	config := replicator.Config{
		Host:     viper.GetString("host"),
		Port:     uint16(viper.GetInt("port")),
		Database: viper.GetString("dbname"),
		User:     viper.GetString("user"),
		Password: viper.GetString("password"),
		Group:    viper.GetString("group"),
		Schema:   viper.GetString("schema"),
		Tables:   viper.GetStringSlice("tables"),
	}

	natsURL := viper.GetString("nats-url")
	natsClient, err := pgflonats.NewNATSClient(natsURL, config.Group, config.Group)
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

func runWorker(cmd *cobra.Command, args []string) {
	group := viper.GetString("group")
	natsURL := viper.GetString("nats-url")
	rulesConfigPath := viper.GetString("rules-config")
	sinkType := cmd.Use

	// Create NATS client
	natsClient, err := pgflonats.NewNATSClient(natsURL, fmt.Sprintf("pgflo_%s_stream", group), group)
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

	sink, err := createSink(sinkType)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sink")
	}

	w := worker.NewWorker(natsClient, ruleEngine, sink, group)
	if err := w.Start(cmd.Context()); err != nil {
		log.Fatal().Err(err).Msg("Worker failed")
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
			viper.GetString("postgres-host"),
			viper.GetInt("postgres-port"),
			viper.GetString("postgres-dbname"),
			viper.GetString("postgres-user"),
			viper.GetString("postgres-password"),
			viper.GetBool("postgres-sync-schema"),
			viper.GetString("host"),
			viper.GetInt("port"),
			viper.GetString("dbname"),
			viper.GetString("user"),
			viper.GetString("password"),
		)
	case "webhook":
		return sinks.NewWebhookSink(
			viper.GetString("webhook-url"),
		)
	default:
		return nil, fmt.Errorf("unknown sink type: %s", sinkType)
	}
}

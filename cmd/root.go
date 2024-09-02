package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shayonj/pg_flo/pkg/replicator"
	"github.com/shayonj/pg_flo/pkg/rules"
	"github.com/shayonj/pg_flo/pkg/sinks"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "pg_flo",
		Short: "The easiest way to move and transform data from PostgreSQL",
	}

	streamCmd = &cobra.Command{
		Use:   "stream",
		Short: "Start streaming changes",
		Long:  `Stream inserts, updates, and deletes from PostgreSQL tables to a sink`,
	}

	copyAndStreamCmd = &cobra.Command{
		Use:   "copy-and-stream",
		Short: "Start copy and stream operation",
		Long:  `Copy existing data and stream changes from PostgreSQL tables to a sink`,
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pg_flo.yaml)")

	commonFlags := func(cmd *cobra.Command) {
		cmd.Flags().String("host", "", "PostgreSQL host")
		cmd.Flags().Int("port", 5432, "PostgreSQL port")
		cmd.Flags().String("dbname", "", "PostgreSQL database name")
		cmd.Flags().String("user", "", "PostgreSQL user")
		cmd.Flags().String("password", "", "PostgreSQL password")
		cmd.Flags().String("group", "", "Group name for replication")
		cmd.Flags().String("schema", "public", "PostgreSQL schema")
		cmd.Flags().StringSlice("tables", []string{}, "Tables to replicate")
		cmd.Flags().String("status-dir", "/tmp", "Directory to store status files")
		cmd.Flags().String("rules-config", "", "Path to rules configuration file")
		cmd.Flags().Bool("track-ddl", false, "Enable DDL tracking")
	}

	addSinkCommands := func(parentCmd *cobra.Command, isCopyAndStream bool) {
		stdoutCmd := &cobra.Command{
			Use:   "stdout",
			Short: "Sync changes to STDOUT",
			Run:   createRunFunc("stdout", isCopyAndStream),
		}
		commonFlags(stdoutCmd)
		if isCopyAndStream {
			stdoutCmd.Flags().Int("max-copy-workers", 4, "Maximum number of parallel workers for copy operation")
		}

		fileCmd := &cobra.Command{
			Use:   "file",
			Short: "Sync changes to file (rotated by size)",
			Run:   createRunFunc("file", isCopyAndStream),
		}
		commonFlags(fileCmd)
		fileCmd.Flags().String("output-dir", "/tmp", "Output directory for file sink")
		if isCopyAndStream {
			fileCmd.Flags().Int("max-copy-workers", 4, "Maximum number of parallel workers for copy operation")
		}

		postgresCmd := &cobra.Command{
			Use:   "postgres",
			Short: "Sync changes to another PostgreSQL database",
			Run:   createRunFunc("postgres", isCopyAndStream),
		}
		commonFlags(postgresCmd)
		postgresCmd.Flags().String("target-host", "", "Target PostgreSQL host")
		postgresCmd.Flags().Int("target-port", 5432, "Target PostgreSQL port")
		postgresCmd.Flags().String("target-dbname", "", "Target PostgreSQL database name")
		postgresCmd.Flags().String("target-user", "", "Target PostgreSQL user")
		postgresCmd.Flags().String("target-password", "", "Target PostgreSQL password")
		postgresCmd.Flags().Bool("sync-schema", false, "Sync schema from source to target")
		if isCopyAndStream {
			postgresCmd.Flags().Int("max-copy-workers", 4, "Maximum number of parallel workers for copy operation")
		}

		webhookCmd := &cobra.Command{
			Use:   "webhook",
			Short: "Sync changes to another Webhook",
			Run:   createRunFunc("webhook", isCopyAndStream),
		}
		commonFlags(webhookCmd)
		webhookCmd.Flags().String("webhook-url", "", "Webhook URL to send data")
		if isCopyAndStream {
			webhookCmd.Flags().Int("max-copy-workers", 4, "Maximum number of parallel workers for copy operation")
		}

		parentCmd.AddCommand(stdoutCmd, fileCmd, postgresCmd, webhookCmd)
	}

	addSinkCommands(streamCmd, false)
	addSinkCommands(copyAndStreamCmd, true)

	rootCmd.AddCommand(streamCmd, copyAndStreamCmd)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName(".pg_flo")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("PG")

	viper.SetDefault("status-dir", "/tmp/")
	viper.SetDefault("host", "")
	viper.SetDefault("port", 5432)
	viper.SetDefault("dbname", "")
	viper.SetDefault("user", "")
	viper.SetDefault("password", "")
	viper.SetDefault("group", "")
	viper.SetDefault("schema", "public")
	viper.SetDefault("tables", []string{})
	viper.SetDefault("rules-config", "")
	viper.SetDefault("track-ddl", false)
	viper.SetDefault("webhook-url", "")

	if err := viper.ReadInConfig(); err == nil {
		log.Info().Str("config_file", viper.ConfigFileUsed()).Msg("Using config file")
	} else {
		log.Warn().Msg("No config file found, using default or environment variables")
	}
}

func createRunFunc(sinkType string, isCopyAndStream bool) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, _ []string) {
		config := createReplicatorConfig(cmd)
		sink := createSink(cmd, sinkType)
		ruleEngine := createRuleEngine(cmd)

		maxCopyWorkers := 0
		if isCopyAndStream {
			maxCopyWorkers = getIntValue(cmd, "max-copy-workers")
		}

		r, err := replicator.NewReplicator(config, sink, isCopyAndStream, maxCopyWorkers, ruleEngine)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create replicator")
			os.Exit(1)
		}

		err = r.CreatePublication()
		if err != nil {
			log.Error().Err(err).Msg("Failed to create publication")
			os.Exit(1)
		}

		err = r.StartReplication()
		if err != nil {
			log.Error().Err(err).Msg("Failed to start replication")
			os.Exit(1)
		}
	}
}

func createReplicatorConfig(cmd *cobra.Command) replicator.Config {
	return replicator.Config{
		Host:     getStringValue(cmd, "host"),
		Port:     uint16(getIntValue(cmd, "port")),
		Database: getStringValue(cmd, "dbname"),
		User:     getStringValue(cmd, "user"),
		Password: getStringValue(cmd, "password"),
		Group:    getStringValue(cmd, "group"),
		Schema:   getStringValue(cmd, "schema"),
		Tables:   getStringSliceValue(cmd, "tables"),
		TrackDDL: getBoolValue(cmd, "track-ddl"),
	}
}

func createSink(cmd *cobra.Command, sinkType string) sinks.Sink {
	statusDir := getStringValue(cmd, "status-dir")
	outputDir := getStringValue(cmd, "output-dir")

	var sink sinks.Sink
	var err error

	switch sinkType {
	case "stdout":
		sink, err = sinks.NewStdoutSink(statusDir)
	case "file":
		sink, err = sinks.NewFileSink(statusDir, outputDir)
	case "postgres":
		targetHost := getStringValue(cmd, "target-host")
		targetPort := getIntValue(cmd, "target-port")
		targetDBName := getStringValue(cmd, "target-dbname")
		targetUser := getStringValue(cmd, "target-user")
		targetPassword := getStringValue(cmd, "target-password")
		syncSchema := getBoolValue(cmd, "sync-schema")
		sourceHost := getStringValue(cmd, "host")
		sourcePort := getIntValue(cmd, "port")
		sourceDBName := getStringValue(cmd, "dbname")
		sourceUser := getStringValue(cmd, "user")
		sourcePassword := getStringValue(cmd, "password")
		sink, err = sinks.NewPostgresSink(targetHost, targetPort, targetDBName, targetUser, targetPassword, syncSchema, sourceHost, sourcePort, sourceDBName, sourceUser, sourcePassword)
	case "webhook":
		webhookURL := getStringValue(cmd, "webhook-url")
		sink, err = sinks.NewWebhookSink(statusDir, webhookURL)
	default:
		log.Error().Str("sink", sinkType).Msg("Invalid sink type")
		os.Exit(1)
	}

	if err != nil {
		log.Error().Err(err).Msg("Failed to create sink")
		os.Exit(1)
	}

	return sink
}

func createRuleEngine(cmd *cobra.Command) *rules.RuleEngine {
	rulesConfigPath := getStringValue(cmd, "rules-config")
	if rulesConfigPath == "" {
		return nil
	}

	config, err := loadRulesConfig(rulesConfigPath)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load rules configuration")
		os.Exit(1)
	}

	ruleEngine := rules.NewRuleEngine()
	if err := ruleEngine.LoadRules(config); err != nil {
		log.Error().Err(err).Msg("Failed to load rules")
		os.Exit(1)
	}

	return ruleEngine
}

func loadRulesConfig(filePath string) (rules.Config, error) {
	var config rules.Config

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, nil
}

func getStringValue(cmd *cobra.Command, key string) string {
	if cmd.Flags().Changed(key) {
		val, _ := cmd.Flags().GetString(key)
		return val
	}
	return viper.GetString(key)
}

func getIntValue(cmd *cobra.Command, key string) int {
	if cmd.Flags().Changed(key) {
		val, _ := cmd.Flags().GetInt(key)
		return val
	}
	return viper.GetInt(key)
}

func getBoolValue(cmd *cobra.Command, key string) bool {
	if cmd.Flags().Changed(key) {
		val, _ := cmd.Flags().GetBool(key)
		return val
	}
	return viper.GetBool(key)
}

func getStringSliceValue(cmd *cobra.Command, key string) []string {
	if cmd.Flags().Changed(key) {
		val, _ := cmd.Flags().GetStringSlice(key)
		return val
	}
	return viper.GetStringSlice(key)
}

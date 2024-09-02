package replicator

import "fmt"

// Config holds the configuration for the replicator
type Config struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
	Group    string
	Schema   string
	Tables   []string
	TrackDDL bool
}

// ConnectionString generates and returns a PostgreSQL connection string
func (c Config) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}

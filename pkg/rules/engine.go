package rules

import (
	"fmt"

	"github.com/shayonj/pg_flo/pkg/utils"
)

// AddRule adds a new rule for the specified table
func (re *RuleEngine) AddRule(tableName string, rule Rule) {
	re.mutex.Lock()
	defer re.mutex.Unlock()
	re.Rules[tableName] = append(re.Rules[tableName], rule)
}

// ApplyRules applies all rules for the specified table to the given data
func (re *RuleEngine) ApplyRules(tableName string, data map[string]utils.CDCValue, operation OperationType) (map[string]utils.CDCValue, error) {
	re.mutex.RLock()
	defer re.mutex.RUnlock()

	rules, exists := re.Rules[tableName]
	if !exists {
		logger.Info().Str("table", tableName).Msg("No rules found for table")
		return data, nil // No rules for this table
	}

	logger.Info().
		Str("table", tableName).
		Str("operation", string(operation)).
		Int("ruleCount", len(rules)).
		Msg("Applying rules")

	result := make(map[string]utils.CDCValue)
	for k, v := range data {
		result[k] = v
	}

	for _, rule := range rules {
		var err error
		result, err = rule.Apply(result, operation)
		if err != nil {
			return nil, fmt.Errorf("error applying rule: %w", err)
		}
		if result == nil {
			return nil, nil // Rule filtered out the entire row
		}
	}

	return result, nil
}

// LoadRules loads rules from the provided configuration
func (re *RuleEngine) LoadRules(config Config) error {
	for tableName, ruleConfigs := range config.Tables {
		logger.Info().Str("table", tableName).Msg("Loading rules for table")
		for i, ruleConfig := range ruleConfigs {
			rule, err := createRule(tableName, ruleConfig)
			if err != nil {
				return fmt.Errorf("error creating rule for table %s: %w", tableName, err)
			}
			logger.Info().
				Str("table", tableName).
				Int("ruleIndex", i+1).
				Str("ruleType", fmt.Sprintf("%T", rule)).
				Msg("Created rule")
			re.AddRule(tableName, rule)
		}
	}
	return nil
}

// createRule creates a new rule based on the provided configuration
func createRule(tableName string, config RuleConfig) (Rule, error) {
	logger.Info().
		Str("table", tableName).
		Str("ruleType", config.Type).
		Msg("Creating rule")
	switch config.Type {
	case "transform":
		return NewTransformRule(tableName, config.Column, config.Parameters)
	case "filter":
		return NewFilterRule(tableName, config.Column, config.Parameters)
	default:
		return nil, fmt.Errorf("unknown rule type: %s", config.Type)
	}
}

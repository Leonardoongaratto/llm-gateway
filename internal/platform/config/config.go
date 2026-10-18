package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type MissingEnvError struct {
	Missing []string
}

func (e *MissingEnvError) Error() string {
	return fmt.Sprintf("missing required environment variables: %s", strings.Join(e.Missing, ", "))
}

func parseEnvTag(prefix string, tag string) (envKey string, isRequired bool) {
	parts := strings.Split(tag, ",")
	envKey = prefix + parts[0]
	for _, p := range parts[1:] {
		if strings.TrimSpace(p) == "required" {
			isRequired = true
		}
	}
	return envKey, isRequired
}

func resolveEnvValue(prefix string, field reflect.StructField) (string, string, bool) {
	tag := field.Tag.Get("env")
	if tag == "" {
		return "", "", false
	}

	envKey, isRequired := parseEnvTag(prefix, tag)
	envVal, exists := os.LookupEnv(envKey)
	defaultVal := field.Tag.Get("default")

	if !exists || strings.TrimSpace(envVal) == "" {
		switch {
		case defaultVal != "":
			return defaultVal, "", true
		case isRequired:
			return "", envKey, false
		default:
			return "", "", false
		}
	}

	return envVal, "", true
}

// LoadFromEnv loads environment variables with the given prefix into a new instance of T.
func LoadFromEnv[T any](prefix string) (*T, error) {
	var cfg T
	val := reflect.ValueOf(&cfg).Elem()
	typ := val.Type()

	var missing []string

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		envVal, missingKey, ok := resolveEnvValue(prefix, field)
		if missingKey != "" {
			missing = append(missing, missingKey)
			continue
		}
		if !ok {
			continue
		}

		if err := setFieldValue(val.Field(i), envVal); err != nil {
			tagParts := strings.Split(field.Tag.Get("env"), ",")
			return nil, fmt.Errorf("failed to parse environment variable %s%s: %w", prefix, tagParts[0], err)
		}
	}

	if len(missing) > 0 {
		return nil, &MissingEnvError{Missing: missing}
	}

	return &cfg, nil
}

func setFieldValue(field reflect.Value, raw string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if field.Type() == reflect.TypeOf(time.Duration(0)) {
			d, err := time.ParseDuration(raw)
			if err != nil {
				return err
			}
			field.SetInt(int64(d))
			return nil
		}
		num, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(num)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return fmt.Errorf("unsupported config field type: %s", field.Type())
	}
	return nil
}

package configuration

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v2"
	"os"
	"strings"
)

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func ReadFile(configFile string) Config {
	f, err := os.Open(configFile)
	if err != nil {
		processError(err)
	}
	defer f.Close()

	cfg := Config{}
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		processError(err)
	}

	return cfg
}

func ReadEnv(cfg *Config) {
	err := envconfig.Process("", cfg)
	if err != nil {
		processError(err)
	}
}

func ReadConfiguration(configFile string) Config {
	cfg := ReadFile(configFile)
	ReadEnv(&cfg)

	return cfg
}

// ReadKafkaConfig reads the configuration into a key value map
func ReadKafkaConfig(configFile string) kafka.ConfigMap {

	var m = make(map[string]kafka.ConfigValue)
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m
}

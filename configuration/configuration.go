package configuration

type Config struct {
	Kafka struct {
		ReadTopic  string `yaml:"topic.read" json:"topic-read,omitempty"`
		WriteTopic string `yaml:"topic.write" json:"topic-write,omitempty"`
	}

	Runtime struct {
		StockSymbols string `yaml:"stock.symbols" json:"stock-symbols,omitempty"`
	}
}

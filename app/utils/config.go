package utils

type Config struct {
	config map[string]any
}

func NewConfig() Config {
	return Config{make(map[string]any)}
}

func (c *Config) SetString(key, value string) {
	c.config[key] = value
}

func (c *Config) SetInt(key string, value int) {
	c.config[key] = value
}

func (c *Config) GetString(key string) (string, bool) {
	v, ok := c.config[key].(string)
	return v, ok
}

func (c *Config) GetInt(key string) (int, bool) {
	v, ok := c.config[key].(int)
	return v, ok
}

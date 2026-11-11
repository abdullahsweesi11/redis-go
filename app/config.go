package main

type Config struct {
	repl map[string]any
	RDB  map[string]any
}

func NewConfig() *Config {
	return &Config{
		repl: make(map[string]any),
		RDB:  make(map[string]any),
	}
}

func (c *Config) SetReplString(key, value string) {
	c.repl[key] = value
}

func (c *Config) SetRDBString(key, value string) {
	c.RDB[key] = value
}

func (c *Config) SetReplInt(key string, value int) {
	c.repl[key] = value
}

func (c *Config) SetRDBInt(key string, value int) {
	c.RDB[key] = value
}

func (c *Config) GetReplString(key string) (string, bool) {
	v, ok := c.repl[key].(string)
	return v, ok
}

func (c *Config) GetReplInt(key string) (int, bool) {
	v, ok := c.repl[key].(int)
	return v, ok
}

func (c *Config) GetRDBString(key string) (string, bool) {
	v, ok := c.RDB[key].(string)
	return v, ok
}

func (c *Config) GetRDBInt(key string) (int, bool) {
	v, ok := c.RDB[key].(int)
	return v, ok
}

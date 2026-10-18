package lint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Config is the user-facing lint settings. Each entry under "rules" is either
// a bool or an object with an optional "enabled" and the rule's options:
//
//	{"rules": {"null-comparison": false, "some-rule": {"enabled": true, "max": 3}}}
//
// The Diagnose cache in package source compares configs with Equal, so update
// it when adding a field.
type Config struct {
	Rules map[string]bool `json:"rules,omitempty"`
	// Options holds each rule's options object without "enabled". It is
	// decoded into the rule's NewOptions value only when the rule runs, so
	// a config can be parsed without knowing the rules.
	Options map[string]json.RawMessage `json:"-"`
}

func (c *Config) UnmarshalJSON(b []byte) error {
	var raw struct {
		Rules map[string]json.RawMessage `json:"rules"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	// A fresh value, not *c, so that decoding never merges into the maps of
	// a previous config.
	var cfg Config
	for _, name := range slices.Sorted(maps.Keys(raw.Rules)) {
		if err := cfg.setRule(name, raw.Rules[name]); err != nil {
			return fmt.Errorf("lint rule %q: %w", name, err)
		}
	}
	*c = cfg
	return nil
}

func (c *Config) setRule(name string, v json.RawMessage) error {
	v = bytes.TrimSpace(v)
	switch {
	case bytes.Equal(v, []byte("true")) || bytes.Equal(v, []byte("false")):
		c.setEnabled(name, bytes.Equal(v, []byte("true")))
		return nil
	case len(v) == 0 || v[0] != '{':
		return errors.New("must be a bool or an object")
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(v, &obj); err != nil {
		return err
	}
	if enabled, ok := obj["enabled"]; ok {
		enabled = bytes.TrimSpace(enabled)
		if !bytes.Equal(enabled, []byte("true")) && !bytes.Equal(enabled, []byte("false")) {
			return errors.New(`"enabled" must be a bool`)
		}
		c.setEnabled(name, bytes.Equal(enabled, []byte("true")))
		delete(obj, "enabled")
	}
	if len(obj) == 0 {
		return nil
	}
	opts, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	if c.Options == nil {
		c.Options = make(map[string]json.RawMessage)
	}
	c.Options[name] = opts
	return nil
}

func (c *Config) setEnabled(name string, enabled bool) {
	if c.Rules == nil {
		c.Rules = make(map[string]bool)
	}
	c.Rules[name] = enabled
}

// Enabled reports whether a is enabled, letting user settings override the default.
func (c Config) Enabled(a *Analyzer) bool {
	if v, ok := c.Rules[a.Name]; ok {
		return v
	}
	return a.DefaultEnabled
}

// Validate reports rules that are not among analyzers and options that the
// rule would reject, so users see typos instead of silently ignored settings.
func (c Config) Validate(analyzers []*Analyzer) error {
	names := append(slices.Collect(maps.Keys(c.Rules)), slices.Collect(maps.Keys(c.Options))...)
	slices.Sort(names)
	for _, name := range slices.Compact(names) {
		i := slices.IndexFunc(analyzers, func(a *Analyzer) bool { return a.Name == name })
		if i < 0 {
			valid := make([]string, len(analyzers))
			for j, a := range analyzers {
				valid[j] = a.Name
			}
			return fmt.Errorf("unknown rule %q (valid: %s)", name, strings.Join(valid, ", "))
		}
		if _, err := c.options(analyzers[i]); err != nil {
			return err
		}
	}
	return nil
}

// options returns the value passed to a as Pass.Options: a's defaults with
// the configured options decoded on top.
func (c Config) options(a *Analyzer) (any, error) {
	raw := c.Options[a.Name]
	if a.NewOptions == nil {
		var obj map[string]json.RawMessage
		if len(raw) > 0 && (json.Unmarshal(raw, &obj) != nil || len(obj) > 0) {
			return nil, fmt.Errorf("rule %q takes no options", a.Name)
		}
		return nil, nil
	}
	opts := a.NewOptions()
	if len(raw) == 0 {
		return opts, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(opts); err != nil {
		return nil, fmt.Errorf("rule %q: invalid options: %w", a.Name, err)
	}
	return opts, nil
}

// Override returns c with the settings in o applied on top. Options are
// merged key by key, so o can change one option without restating the rest.
// Neither c nor o is modified.
func (c Config) Override(o Config) (Config, error) {
	var result Config
	if len(c.Rules)+len(o.Rules) > 0 {
		result.Rules = maps.Clone(c.Rules)
		if result.Rules == nil {
			result.Rules = make(map[string]bool, len(o.Rules))
		}
		maps.Copy(result.Rules, o.Rules)
	}
	if len(c.Options)+len(o.Options) > 0 {
		result.Options = maps.Clone(c.Options)
		if result.Options == nil {
			result.Options = make(map[string]json.RawMessage, len(o.Options))
		}
		for name, opts := range o.Options {
			merged, err := mergeObjects(result.Options[name], opts)
			if err != nil {
				return Config{}, fmt.Errorf("lint rule %q: %w", name, err)
			}
			result.Options[name] = merged
		}
	}
	return result, nil
}

func mergeObjects(base, over json.RawMessage) (json.RawMessage, error) {
	if len(base) == 0 {
		return over, nil
	}
	var b, o map[string]json.RawMessage
	if err := json.Unmarshal(base, &b); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(over, &o); err != nil {
		return nil, err
	}
	if b == nil {
		b = make(map[string]json.RawMessage, len(o))
	}
	maps.Copy(b, o)
	return json.Marshal(b)
}

func (c Config) Equal(o Config) bool {
	return maps.Equal(c.Rules, o.Rules) && maps.EqualFunc(c.Options, o.Options, func(a, b json.RawMessage) bool {
		return bytes.Equal(a, b)
	})
}

// Clone returns a deep copy, so a caller can keep c while others mutate theirs.
func (c Config) Clone() Config {
	result := Config{Rules: maps.Clone(c.Rules)}
	if c.Options != nil {
		result.Options = make(map[string]json.RawMessage, len(c.Options))
		for name, opts := range c.Options {
			result.Options[name] = slices.Clone(opts)
		}
	}
	return result
}

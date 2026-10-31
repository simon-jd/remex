package scheduler

import (
	"fmt"
	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"math"
	"strconv"
	"strings"
)

func ParsePlatform(p *re.Platform) (map[string]string, error) {
	out := map[string]string{}
	if p == nil {
		return out, nil
	}
	for _, prop := range p.Properties {
		if prop.Name == "" {
			return nil, fmt.Errorf("empty platform property")
		}
		if _, ok := out[prop.Name]; ok {
			return nil, fmt.Errorf("duplicate platform property %q", prop.Name)
		}
		out[prop.Name] = prop.Value
	}
	return out, nil
}
func Match(required, actual map[string]string) error {
	for k, v := range required {
		if k == "remex-hw" {
			continue
		}
		if k == "cpu.min" || k == "mem_gb.min" {
			need, err := strconv.ParseFloat(v, 64)
			if err != nil || need <= 0 || math.IsNaN(need) || math.IsInf(need, 0) {
				return fmt.Errorf("invalid requirement %s=%s", k, v)
			}
			key := strings.TrimSuffix(k, ".min")
			value := actual[key]
			if value == "" {
				value = actual[k]
			}
			have, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(have) || math.IsInf(have, 0) || have < need {
				return fmt.Errorf("%s requires %s, worker reports %q", k, v, value)
			}
			continue
		}
		if actual[k] != v {
			return fmt.Errorf("%s requires %q, worker reports %q", k, v, actual[k])
		}
	}
	return nil
}
func hardware(p map[string]string) bool { return p["remex-hw"] == "true" || p["remex-hw"] == "1" }

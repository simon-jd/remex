package regress

import (
	"path"
	"sort"
	"strings"
)

type HardwareTarget struct {
	Label string   `json:"label"`
	Tags  []string `json:"tags"`
}
type SelectionRequest struct {
	Targets         []HardwareTarget `json:"targets"`
	Affected        []string         `json:"affected"`
	ChangedFiles    []string         `json:"changed_files"`
	SelectorError   string           `json:"selector_error,omitempty"`
	PlatformChanged bool             `json:"platform_changed"`
	ImageChanged    bool             `json:"image_changed"`
	DriverChanged   bool             `json:"driver_changed"`
	GlobalPrefixes  []string         `json:"global_prefixes,omitempty"`
}
type Selection struct {
	Targets []string `json:"targets"`
	Full    bool     `json:"full"`
	Reasons []string `json:"reasons"`
}

// SelectHardware selects only explicitly non-cacheable hardware tests. An
// uncertain dependency graph, invalid tags, or global inputs force a full run.
func SelectHardware(r SelectionRequest) Selection {
	out := Selection{Targets: []string{}, Reasons: []string{}}
	affected := map[string]bool{}
	for _, t := range r.Affected {
		affected[t] = true
	}
	hardware := map[string]bool{}
	for _, t := range r.Targets {
		hw, nocache := false, false
		for _, tag := range t.Tags {
			hw = hw || tag == "remex-hw"
			nocache = nocache || tag == "no-remote-cache"
		}
		if hw {
			hardware[t.Label] = true
			if !nocache {
				out.Reasons = append(out.Reasons, t.Label+": remex-hw missing no-remote-cache")
			}
		}
	}
	if r.SelectorError != "" {
		out.Reasons = append(out.Reasons, "selector failed: "+r.SelectorError)
	}
	if r.PlatformChanged || r.ImageChanged || r.DriverChanged {
		out.Reasons = append(out.Reasons, "platform, image or driver changed")
	}
	for _, f := range r.ChangedFiles {
		clean := path.Clean(strings.ReplaceAll(f, "\\", "/"))
		base := path.Base(clean)
		global := base == "MODULE.bazel" ||
			base == "MODULE.bazel.lock" ||
			base == ".bazelrc" ||
			base == ".bazelversion" ||
			base == "environment.lock" ||
			strings.Contains(
				clean,
				"toolchain",
			) || strings.Contains(clean, "platform") || strings.Contains(
			clean,
			"Dockerfile",
		) || strings.HasPrefix(clean, "deploy/")
		for _, p := range r.GlobalPrefixes {
			global = global || strings.HasPrefix(clean, p)
		}
		if global {
			out.Reasons = append(out.Reasons, "global input changed: "+f)
		}
	}
	out.Full = len(out.Reasons) > 0
	for t := range hardware {
		if out.Full || affected[t] {
			out.Targets = append(out.Targets, t)
		}
	}
	sort.Strings(out.Targets)
	sort.Strings(out.Reasons)
	return out
}

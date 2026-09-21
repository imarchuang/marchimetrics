package main

import "testing"

func TestParseRetentionDays(t *testing.T) {
	for in, want := range map[string]int{"0": 0, "": 0, " 0 ": 0, "7d": 7, "30d": 30, "1d": 1} {
		got, err := parseRetentionDays(in)
		if err != nil {
			t.Errorf("parseRetentionDays(%q): unexpected error %s", in, err)
		} else if got != want {
			t.Errorf("parseRetentionDays(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"7", "7h", "xd", "-1d", "d", "7.5d"} {
		if _, err := parseRetentionDays(bad); err == nil {
			t.Errorf("parseRetentionDays(%q) succeeded, want error", bad)
		}
	}
}

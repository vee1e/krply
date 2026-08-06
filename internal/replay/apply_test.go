package replay

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRestConfigFromKubeconfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	content := `apiVersion: v1
kind: Config
clusters:
- name: demo
  cluster:
    server: https://demo.example:6443
contexts:
- name: demo
  context:
    cluster: demo
    user: demo
current-context: demo
users:
- name: demo
  user: {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	cfg, err := restConfig(path, "demo")
	if err != nil {
		t.Fatalf("restConfig: %v", err)
	}
	if cfg.Host != "https://demo.example:6443" {
		t.Errorf("host = %q, want https://demo.example:6443", cfg.Host)
	}
}

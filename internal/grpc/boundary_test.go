package grpc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// 生成 binding 不依赖 SDK 适配器，也不通过父包别名重新导出。
func TestGeneratedBindingBoundary(t *testing.T) {
	const generatedImport = "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	for _, dir := range []string{".", "gen"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			path := filepath.Join(dir, name)
			if dir == "." && strings.HasSuffix(name, ".pb.go") {
				t.Errorf("generated binding outside gen: %s", path)
			}
			if dir == "gen" && name != "doc.go" && !strings.HasSuffix(name, ".pb.go") {
				t.Errorf("unexpected file in generated directory: %s", path)
			}
			if !strings.HasSuffix(name, ".go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			aliases := map[string]bool{}
			for _, spec := range file.Imports {
				value, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				if dir == "gen" && strings.HasPrefix(value, "github.com/836145715/brickly-sdk-go") {
					t.Errorf("generated binding depends on SDK: %s imports %s", path, value)
				}
				if value == generatedImport {
					alias := "runtimev1"
					if spec.Name != nil {
						alias = spec.Name.Name
					}
					aliases[alias] = true
				}
			}
			if dir == "gen" && file.Name.Name != "runtimev1" {
				t.Errorf("unexpected generated package: %s", path)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				spec, ok := node.(*ast.TypeSpec)
				if !ok || !spec.Assign.IsValid() {
					return true
				}
				ast.Inspect(spec.Type, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if ok {
						id, ok := selector.X.(*ast.Ident)
						if ok && aliases[id.Name] {
							t.Errorf("adapter re-exports generated type: %s in %s", spec.Name, path)
						}
					}
					return true
				})
				return true
			})
		}
	}
}

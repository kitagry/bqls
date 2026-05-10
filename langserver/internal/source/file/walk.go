package file

import (
	"reflect"

	googlesql "github.com/goccy/go-googlesql"
)

// Walk traverses an ASTNode tree in depth-first order, calling fn for each node.
// If fn returns an error, Walk stops and returns that error.
func Walk(node googlesql.ASTNode, fn func(googlesql.ASTNode) error) error {
	if node == nil {
		return nil
	}
	// Handle typed nil (e.g. (*ASTScript)(nil) passed as interface).
	if v := reflect.ValueOf(node); v.Kind() == reflect.Ptr && v.IsNil() {
		return nil
	}
	if err := fn(node); err != nil {
		return err
	}
	n, err := node.NumChildren()
	if err != nil {
		return nil
	}
	for i := int32(0); i < n; i++ {
		child, err := node.Child(i)
		if err != nil || child == nil {
			continue
		}
		if err := Walk(child, fn); err != nil {
			return err
		}
	}
	return nil
}

// WalkResolved traverses a ResolvedNode tree in depth-first order.
func WalkResolved(node googlesql.ResolvedNode, fn func(googlesql.ResolvedNode) error) error {
	if node == nil {
		return nil
	}
	if err := fn(node); err != nil {
		return err
	}
	children, err := node.GetChildNodes()
	if err != nil {
		return nil
	}
	for _, child := range children {
		if child == nil {
			continue
		}
		if err := WalkResolved(child, fn); err != nil {
			return err
		}
	}
	return nil
}

// locStart returns the start byte offset of an ASTNode's parse location, or -1 if unavailable.
func locStart(node googlesql.ASTNode) int {
	loc, err := node.GetParseLocationRange()
	if err != nil || loc == nil {
		return -1
	}
	start, err := loc.Start()
	if err != nil || start == nil {
		return -1
	}
	off, err := start.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

// locEnd returns the end byte offset of an ASTNode's parse location, or -1 if unavailable.
func locEnd(node googlesql.ASTNode) int {
	loc, err := node.GetParseLocationRange()
	if err != nil || loc == nil {
		return -1
	}
	end, err := loc.End()
	if err != nil || end == nil {
		return -1
	}
	off, err := end.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

// resolvedLocStart returns the start byte offset of a ResolvedNode, or -1.
func resolvedLocStart(node googlesql.ResolvedNode) int {
	loc, err := node.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return -1
	}
	start, err := loc.Start()
	if err != nil || start == nil {
		return -1
	}
	off, err := start.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

// resolvedLocEnd returns the end byte offset of a ResolvedNode, or -1.
func resolvedLocEnd(node googlesql.ResolvedNode) int {
	loc, err := node.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return -1
	}
	end, err := loc.End()
	if err != nil || end == nil {
		return -1
	}
	off, err := end.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

// ParseLocStart/End are exported wrappers for use from other packages.
func ParseLocStart(loc *googlesql.ParseLocationRange) int { return parseLocStart(loc) }
func ParseLocEnd(loc *googlesql.ParseLocationRange) int   { return parseLocEnd(loc) }

// parseLocStart/End helpers for *googlesql.ParseLocationRange (already in memory).
func parseLocStart(loc *googlesql.ParseLocationRange) int {
	if loc == nil {
		return -1
	}
	start, err := loc.Start()
	if err != nil || start == nil {
		return -1
	}
	off, err := start.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

func parseLocEnd(loc *googlesql.ParseLocationRange) int {
	if loc == nil {
		return -1
	}
	end, err := loc.End()
	if err != nil || end == nil {
		return -1
	}
	off, err := end.GetByteOffset()
	if err != nil {
		return -1
	}
	return int(off)
}

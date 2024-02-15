package plan

import "fmt"

var ruleRegistry []Rule

const (
	StopPushdownRuleID    = 0x01
	PruneColumnRuleID     = 0x02
	ExtractPushDownRuleID = 0x04
)

func init() {
	var id uint64

	ruleRegistry = []Rule{
		NewStopPushdownRule(),
		NewPruneColumn(),
		NewExtractPushDownRule(),
	}
	for _, rule := range ruleRegistry {
		if id&rule.ID() != 0 {
			panic(fmt.Sprintf("rule ID collision: %s", rule.Name()))
		}
		id |= rule.ID()
	}
}

func Optimize(root *Scope) *Scope {
	for _, rule := range ruleRegistry {
		root = root.applyRule(rule)
	}
	for i := range root.Children {
		root.Children[i] = Optimize(root.Children[i])
	}
	return root
}

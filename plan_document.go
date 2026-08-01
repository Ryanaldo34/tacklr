package tacklr

import "strings"

// planDocumentPrefix identifies durable plan messages so Fit can protect them.
const planDocumentPrefix = "PROJECT PLAN\n────────────\n"

func formatPlanDocument(raw string) string {
	return planDocumentPrefix + raw
}

func isPlanDocument(m *Message) bool {
	return m != nil && strings.HasPrefix(m.Content, planDocumentPrefix)
}

func rawPlanFromDocumentMessage(m *Message) string {
	return strings.TrimPrefix(m.Content, planDocumentPrefix)
}

func buildPlanDocumentMessage(raw string) *Message {
	return &Message{
		Role:    RoleDeveloper,
		Content: formatPlanDocument(raw),
	}
}

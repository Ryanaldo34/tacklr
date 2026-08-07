package agentbench

import "github.com/google/uuid"

// AllCases returns every built-in benchmark case (seed data embedded in Go).
func AllCases() []Case {
	var out []Case
	out = append(out, planInterruptCases()...)
	out = append(out, memoryCases()...)
	out = append(out, multihopCases()...)
	out = append(out, toolDomainCases()...)
	out = append(out, webCases()...)
	return out
}

// CasesForSuite filters AllCases by suite id (empty / "all" → all).
func CasesForSuite(suite string) []Case {
	if suite == "" || suite == "all" {
		return AllCases()
	}
	var out []Case
	for _, c := range AllCases() {
		if c.Suite == suite {
			out = append(out, c)
		}
	}
	return out
}

func planInterruptCases() []Case {
	return []Case{
		{
			ID:    "plan_interrupt-ask-before-save",
			Suite: SuitePlanInterrupt,
			Turns: []string{
				`I want you to remember a durable work preference, but only after asking me whether to save it as a memory or a fact.
Preference: "Ship release notes the same day as deploy."
1) Create a short plan with todos.
2) Use ask_user_choice with options that include "Save as memory" and "Save as fact".
3) After I answer, save with the matching save tool and confirm what you stored.`,
			},
			InterruptChoiceTitle: "Save as memory",
			Expect: Expect{
				MustInterrupt: true,
				MustTools: [][]string{
					{"create_plan"},
					{"ask_user_choice"},
					{"save_memory"},
				},
				FinalContainsAny:   []string{"release notes", "memory", "saved", "stored"},
				BrainKind:          "Memory",
				BrainQuery:         "release notes",
				BrainTitleContains: "release",
			},
		},
		{
			ID:    "plan_interrupt-complete-todos",
			Suite: SuitePlanInterrupt,
			Turns: []string{
				`Create a plan with at least two todos for organizing my notes about "Website redesign Orion", then mark each todo complete as you finish checking whether any notes exist (use search or find_objects). Summarize what you found.`,
			},
			Seed: WorldMeetingPrep(),
			Expect: Expect{
				MustTools: [][]string{
					{"create_plan"},
					{"complete_todo"},
					{"search", "find_objects", "find_exact"},
				},
				FinalContainsAny: []string{"Orion", "redesign", "legal", "Alex"},
			},
		},
	}
}

func memoryCases() []Case {
	return []Case{
		{
			ID:    "memory-save-and-recall",
			Suite: SuiteMemory,
			Turns: []string{
				`Please save as a durable memory (save_memory): my manager Alex prefers async written updates instead of status meetings. Confirm after saving.`,
				`Without me repeating it: what does Alex prefer for status updates? Use knowledge tools if needed.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"save_memory"},
					{"search", "find_objects", "find_exact", "read"},
				},
				FinalContainsAny:   []string{"async"},
				BrainKind:          "Memory",
				BrainQuery:         "Alex",
				BrainTitleContains: "Alex",
			},
		},
		{
			ID:             "memory-session-restore",
			Suite:          SuiteMemory,
			RestoreSession: true,
			Turns: []string{
				`Save with save_memory that the Orion website launch target is end of month. Confirm saved.`,
				`What is the Orion website launch target? Look up stored knowledge if unsure.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"save_memory"},
				},
				FinalContainsAny: []string{"end of month", "month"},
				BrainKind:        "Memory",
				BrainQuery:       "Orion",
			},
		},
	}
}

func multihopCases() []Case {
	return []Case{
		{
			ID:    "multihop-orion-blocker-and-owner",
			Suite: SuiteMultihopQA,
			Seed:  WorldMeetingPrep(),
			Turns: []string{
				`Using only the knowledge base (search, find_objects, expand as needed), answer:
1) What is blocking the Website redesign Orion homepage copy?
2) Who is the engineering manager linked to Orion?
Create a plan, use tools, then answer both clearly.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"create_plan", "search", "find_objects", "expand"},
					{"search", "find_objects", "expand", "find_exact", "read"},
				},
				FinalContainsAll: []string{"legal", "Alex"},
				GoldEvidenceIDs:  []uuid.UUID{IDNoteLegal, IDPersonAlex, IDProjectOrion},
			},
		},
		{
			ID:    "multihop-alex-preference-from-notes",
			Suite: SuiteMultihopQA,
			Seed:  WorldMeetingPrep(),
			Turns: []string{
				`What communication preference does Alex have according to stored notes? Use find_objects or search, and expand from Alex or the 1:1 note if helpful.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"search", "find_objects", "find_exact", "expand", "read"},
				},
				FinalContainsAny: []string{"async"},
				GoldEvidenceIDs:  []uuid.UUID{IDNoteAsync, IDPersonAlex},
			},
		},
	}
}

func toolDomainCases() []Case {
	return []Case{
		{
			ID:    "domain-prep-brief-orion",
			Suite: SuiteToolDomain,
			Seed:  WorldMeetingPrep(),
			Turns: []string{
				`I have a 1:1 with Alex about Website redesign Orion tomorrow.
Use the knowledge base to prepare a short brief covering: (1) Alex's communication preference, (2) the main Orion risk/blocker, (3) who else works on Orion.
Save one Discovery titled something with "Orion 1:1 brief" summarizing the prep.
Use a plan with todos.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"create_plan"},
					{"search", "find_objects", "expand", "find_exact"},
					{"save_discovery"},
				},
				FinalContainsAny:   []string{"async", "legal", "Sam"},
				BrainKind:          "Discovery",
				BrainQuery:         "Orion",
				BrainTitleContains: "Orion",
			},
		},
		{
			ID:    "domain-link-fact-to-project",
			Suite: SuiteToolDomain,
			Seed:  WorldMeetingPrep(),
			Turns: []string{
				`Create a Fact titled "Trademark checklist pending" about counsel needing a trademark checklist for Orion homepage.
Link that fact to the Website redesign Orion project with relation "about" and a short note.
Confirm both save and link succeeded.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"save_fact"},
					{"link", "find_objects"},
				},
				FinalContainsAny:   []string{"trademark", "link", "Orion"},
				BrainKind:          "Fact",
				BrainQuery:         "Trademark",
				BrainTitleContains: "Trademark",
			},
		},
	}
}

func webCases() []Case {
	return []Case{
		{
			ID:          "web-internal-plus-public",
			Suite:       SuiteWebAugmented,
			RequiresExa: true,
			Seed: SeedWorld{
				Objects: []SeedObject{
					{
						Kind: "Document", Title: "Internal: use UTC for all release timestamps",
						Summary: "Engineering standard",
						Content: "All release notes and deploy markers must use UTC, not local time.",
					},
				},
			},
			Turns: []string{
				`Two parts:
1) From the knowledge base only: what timezone standard do we use for release timestamps?
2) Use web_search to find the current UTC offset relationship or a one-line definition of UTC (Coordinated Universal Time) from the public web.
Answer both. You must call web_search for part 2.`,
			},
			Expect: Expect{
				MustTools: [][]string{
					{"search", "find_objects", "find_exact"},
					{"web_search"},
				},
				FinalContainsAny: []string{"UTC"},
			},
		},
	}
}

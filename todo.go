package tacklr

// Todo is one item in an agent plan list (create_plan / plan_update stream data).
type Todo struct {
	Title       string     `json:"title" desc:"Todo title. Must be unique in the list."`
	Status      TodoStatus `json:"status" desc:"pending, in_progress, or completed."`
	Description string     `json:"description" desc:"Objective, expected outcomes, and acceptance criteria."`
}

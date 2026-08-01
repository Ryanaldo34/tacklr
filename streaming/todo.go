package streaming

// Todo is one item in an agent plan list (create_plan / plan_update stream data).
type Todo struct {
	Title       string     `json:"title"`
	Status      TodoStatus `json:"status"`
	Description string     `json:"description"`
}

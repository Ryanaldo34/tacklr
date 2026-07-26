package control

import "github.com/ryanaldo34/tacklr/streaming"

type Todo struct {
	Title       string               `json:"title"`
	Status      streaming.TodoStatus `json:"status"`
	Description string               `json:"description"`
}

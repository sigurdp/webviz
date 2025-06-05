package tasks

import (
	"surface_query/utils"
)

type TaskDeps struct {
	tempUserStoreFactory *utils.TempUserStoreFactory
}

func NewTaskDeps(tempUserStoreFactory *utils.TempUserStoreFactory) *TaskDeps {

	return &TaskDeps{tempUserStoreFactory: tempUserStoreFactory}
}

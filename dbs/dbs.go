package dbs

type Mutation interface {
	DML()
}

type Statement struct {
	SQL    string
	Params map[string]interface{}
}

type UserDefinedDML interface {
	GetStatements() []Statement
}

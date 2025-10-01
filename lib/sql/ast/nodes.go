package ast

// Node represents any AST element that can accept a Visitor.
type Node interface {
	Accept(Visitor)
}

// Statement is the root type for SQL statements.
type Statement interface {
	Node
	statementNode()
}

// Expr models SQL expressions.
type Expr interface {
	Node
	exprNode()
}

// TableExpr represents selectable table expressions.
type TableExpr interface {
	Node
	tableNode()
}

// SelectItem describes an item in the SELECT list.
type SelectItem struct {
	Expr  Expr
	Alias string
}

// OrderItem represents ORDER BY terms.
type OrderItem struct {
	Expr      Expr
	Direction OrderDirection
}

// OrderDirection enumerates ORDER BY directions.
type OrderDirection string

const (
	Ascending  OrderDirection = "ASC"
	Descending OrderDirection = "DESC"
)

// LimitClause captures LIMIT/OFFSET values.
type LimitClause struct {
	Count  Expr // can be nil for OFFSET only
	Offset Expr
}

// SelectStatement captures a SELECT query.
type SelectStatement struct {
	With     *WithClause
	Distinct bool
	Columns  []SelectItem
	From     TableExpr
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderItem
	Limit    *LimitClause
	SetOps   []SetOperation
}

func (*SelectStatement) statementNode() {}

// WithClause stores common table expressions.
type WithClause struct {
	Recursive bool
	CTEs      []CommonTableExpression
}

// CommonTableExpression represents a single named subquery.
type CommonTableExpression struct {
	Name    *Identifier
	Columns []*Identifier
	Select  *SelectStatement
}

// InsertStatement models INSERT queries.
type InsertStatement struct {
	Table   *TableName
	Columns []*Identifier
	Rows    [][]Expr
	Select  *SelectStatement
}

func (*InsertStatement) statementNode() {}

// UpdateStatement models UPDATE queries.
type UpdateStatement struct {
	Table       TableExpr
	Assignments []Assignment
	From        TableExpr
	Where       Expr
}

func (*UpdateStatement) statementNode() {}

// DeleteStatement models DELETE queries.
type DeleteStatement struct {
	Table TableExpr
	Using TableExpr
	Where Expr
}

func (*DeleteStatement) statementNode() {}

// CreateViewStatement models CREATE VIEW statements.
type CreateViewStatement struct {
	OrReplace    bool
	IfNotExists  bool
	Materialized bool
	Name         *Identifier
	Columns      []*Identifier
	Select       *SelectStatement
}

func (*CreateViewStatement) statementNode() {}

// DropViewStatement models DROP VIEW statements.
type DropViewStatement struct {
	Materialized bool
	IfExists     bool
	Name         *Identifier
}

func (*DropViewStatement) statementNode() {}

// DescribeTarget enumerates the possible entities a DESCRIBE statement can address.
type DescribeTarget string

const (
	DescribeTable DescribeTarget = "TABLE"
	DescribeView  DescribeTarget = "VIEW"
)

// DescribeStatement models DESCRIBE TABLE/VIEW statements.
type DescribeStatement struct {
	Target DescribeTarget
	Name   *Identifier
}

func (*DescribeStatement) statementNode() {}

// ShowTablesStatement models SHOW TABLES commands.
type ShowTablesStatement struct{}

func (*ShowTablesStatement) statementNode() {}

// ShowViewsStatement models SHOW VIEWS commands.
type ShowViewsStatement struct{}

func (*ShowViewsStatement) statementNode() {}

// Assignment represents column=expr pairs in UPDATE SET.
type Assignment struct {
	Column *Identifier
	Value  Expr
}

// Identifier models possibly qualified identifiers.
type Identifier struct {
	Parts []string
}

func (Identifier) exprNode()  {}
func (Identifier) tableNode() {}

// TableName represents a table reference with optional alias.
type TableName struct {
	Name  *Identifier
	Alias string
}

func (*TableName) tableNode() {}

// SubqueryTable wraps a subquery used as table expression.
type SubqueryTable struct {
	Select *SelectStatement
	Alias  string
}

func (*SubqueryTable) tableNode() {}

// JoinType enumerates supported ANSI join types.
type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinFull  JoinType = "FULL"
	JoinCross JoinType = "CROSS"
)

// JoinExpr represents a JOIN expression.
type JoinExpr struct {
	Left      TableExpr
	Right     TableExpr
	Type      JoinType
	Condition JoinCondition
}

func (*JoinExpr) tableNode() {}

// JoinCondition captures ON clauses.
type JoinCondition struct {
	On Expr
}

// SetOperator describes set combination types.
type SetOperator string

const (
	SetOpUnion     SetOperator = "UNION"
	SetOpIntersect SetOperator = "INTERSECT"
	SetOpExcept    SetOperator = "EXCEPT"
)

// SetOperation joins the current SELECT with another via UNION/INTERSECT/EXCEPT.
type SetOperation struct {
	Operator SetOperator
	All      bool
	Select   *SelectStatement
}

// StarExpr denotes the wildcard selector.
type StarExpr struct {
	Table *Identifier
}

func (*StarExpr) exprNode() {}

// Literal kinds.
type (
	NumericLiteral struct{ Value string }
	StringLiteral  struct{ Value string }
	BooleanLiteral struct{ Value bool }
	NullLiteral    struct{}
	Placeholder    struct{ Symbol string }
)

func (*NumericLiteral) exprNode() {}
func (*StringLiteral) exprNode()  {}
func (*BooleanLiteral) exprNode() {}
func (*NullLiteral) exprNode()    {}
func (*Placeholder) exprNode()    {}

// BinaryExpr models binary operations like a+b or a AND b.
type BinaryExpr struct {
	Left     Expr
	Operator string
	Right    Expr
}

func (*BinaryExpr) exprNode() {}

// UnaryExpr models prefix operators.
type UnaryExpr struct {
	Operator string
	Expr     Expr
}

func (*UnaryExpr) exprNode() {}

// FuncCall models function invocations.
type FuncCall struct {
	Name     Identifier
	Distinct bool
	Args     []Expr
	Over     *WindowSpecification
}

func (*FuncCall) exprNode() {}

// WindowSpecification describes OVER(...) clauses on function calls.
type WindowSpecification struct {
	PartitionBy []Expr
	OrderBy     []OrderItem
}

// CaseExpr represents simple CASE constructs.
type CaseExpr struct {
	Operand Expr
	When    []WhenClause
	Else    Expr
}

func (*CaseExpr) exprNode() {}

// WhenClause holds CASE branches.
type WhenClause struct {
	Condition Expr
	Result    Expr
}

// BetweenExpr models BETWEEN operations.
type BetweenExpr struct {
	Expr  Expr
	Lower Expr
	Upper Expr
	Not   bool
}

func (*BetweenExpr) exprNode() {}

// InExpr models IN and NOT IN.
type InExpr struct {
	Expr     Expr
	Not      bool
	Subquery *SelectStatement
	List     []Expr
}

func (*InExpr) exprNode() {}

// LikeExpr models LIKE expressions.
type LikeExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
}

func (*LikeExpr) exprNode() {}

// IsNullExpr models IS [NOT] NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) exprNode() {}

// ExistsExpr models EXISTS (subquery).
type ExistsExpr struct {
	Not      bool
	Subquery *SelectStatement
}

func (*ExistsExpr) exprNode() {}

// SubqueryExpr allows scalar subqueries.
type SubqueryExpr struct {
	Select *SelectStatement
}

func (*SubqueryExpr) exprNode() {}

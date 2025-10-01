package ast

// Visitor is implemented by algorithms that walk the AST.
type Visitor interface {
	Visit(Node) Visitor
}

// Accept satisfies Node for SelectStatement.
func (s *SelectStatement) Accept(v Visitor)     { Walk(v, s) }
func (s *InsertStatement) Accept(v Visitor)     { Walk(v, s) }
func (s *UpdateStatement) Accept(v Visitor)     { Walk(v, s) }
func (s *DeleteStatement) Accept(v Visitor)     { Walk(v, s) }
func (s *CreateViewStatement) Accept(v Visitor) { Walk(v, s) }
func (s *DropViewStatement) Accept(v Visitor)   { Walk(v, s) }
func (s *DescribeStatement) Accept(v Visitor)   { Walk(v, s) }
func (s *ShowTablesStatement) Accept(v Visitor) { Walk(v, s) }
func (s *ShowViewsStatement) Accept(v Visitor)  { Walk(v, s) }
func (i *Identifier) Accept(v Visitor)          { Walk(v, i) }
func (t *TableName) Accept(v Visitor)           { Walk(v, t) }
func (t *SubqueryTable) Accept(v Visitor)       { Walk(v, t) }
func (j *JoinExpr) Accept(v Visitor)            { Walk(v, j) }
func (s *StarExpr) Accept(v Visitor)            { Walk(v, s) }
func (n *NumericLiteral) Accept(v Visitor)      { Walk(v, n) }
func (s *StringLiteral) Accept(v Visitor)       { Walk(v, s) }
func (b *BooleanLiteral) Accept(v Visitor)      { Walk(v, b) }
func (n *NullLiteral) Accept(v Visitor)         { Walk(v, n) }
func (p *Placeholder) Accept(v Visitor)         { Walk(v, p) }
func (b *BinaryExpr) Accept(v Visitor)          { Walk(v, b) }
func (u *UnaryExpr) Accept(v Visitor)           { Walk(v, u) }
func (f *FuncCall) Accept(v Visitor)            { Walk(v, f) }
func (c *CaseExpr) Accept(v Visitor)            { Walk(v, c) }
func (b *BetweenExpr) Accept(v Visitor)         { Walk(v, b) }
func (i *InExpr) Accept(v Visitor)              { Walk(v, i) }
func (l *LikeExpr) Accept(v Visitor)            { Walk(v, l) }
func (i *IsNullExpr) Accept(v Visitor)          { Walk(v, i) }
func (e *ExistsExpr) Accept(v Visitor)          { Walk(v, e) }
func (s *SubqueryExpr) Accept(v Visitor)        { Walk(v, s) }

// Walk traverses the AST rooted at node using the provided visitor.
func Walk(v Visitor, node Node) {
	if node == nil || v == nil {
		return
	}
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *SelectStatement:
		if n.With != nil {
			for _, cte := range n.With.CTEs {
				Walk(v, cte.Name)
				for _, col := range cte.Columns {
					Walk(v, col)
				}
				Walk(v, cte.Select)
			}
		}
		for i := range n.Columns {
			if n.Columns[i].Expr != nil {
				Walk(v, n.Columns[i].Expr)
			}
		}
		Walk(v, n.From)
		Walk(v, n.Where)
		for _, g := range n.GroupBy {
			Walk(v, g)
		}
		Walk(v, n.Having)
		for _, o := range n.OrderBy {
			Walk(v, o.Expr)
		}
		if n.Limit != nil {
			Walk(v, n.Limit.Count)
			Walk(v, n.Limit.Offset)
		}
		for _, op := range n.SetOps {
			Walk(v, op.Select)
		}
	case *InsertStatement:
		Walk(v, n.Table)
		for _, col := range n.Columns {
			Walk(v, col)
		}
		for _, row := range n.Rows {
			for _, expr := range row {
				Walk(v, expr)
			}
		}
		Walk(v, n.Select)
	case *UpdateStatement:
		Walk(v, n.Table)
		for i := range n.Assignments {
			Walk(v, n.Assignments[i].Column)
			Walk(v, n.Assignments[i].Value)
		}
		Walk(v, n.From)
		Walk(v, n.Where)
	case *DeleteStatement:
		Walk(v, n.Table)
		Walk(v, n.Using)
		Walk(v, n.Where)
	case *CreateViewStatement:
		Walk(v, n.Name)
		for _, col := range n.Columns {
			Walk(v, col)
		}
		Walk(v, n.Select)
	case *DropViewStatement:
		Walk(v, n.Name)
	case *ShowTablesStatement, *ShowViewsStatement:
		// leaves have no children
	case *Identifier:
		// leaves have no children
	case *TableName:
		Walk(v, n.Name)
	case *SubqueryTable:
		Walk(v, n.Select)
	case *JoinExpr:
		Walk(v, n.Left)
		Walk(v, n.Right)
		Walk(v, n.Condition.On)
	case *StarExpr:
		if n.Table != nil {
			Walk(v, n.Table)
		}
	case *NumericLiteral, *StringLiteral, *BooleanLiteral, *NullLiteral, *Placeholder:
		// leaves
	case *BinaryExpr:
		Walk(v, n.Left)
		Walk(v, n.Right)
	case *UnaryExpr:
		Walk(v, n.Expr)
	case *FuncCall:
		Walk(v, &n.Name)
		for _, arg := range n.Args {
			Walk(v, arg)
		}
		if n.Over != nil {
			for _, expr := range n.Over.PartitionBy {
				Walk(v, expr)
			}
			for _, item := range n.Over.OrderBy {
				Walk(v, item.Expr)
			}
		}
	case *CaseExpr:
		Walk(v, n.Operand)
		for _, when := range n.When {
			Walk(v, when.Condition)
			Walk(v, when.Result)
		}
		Walk(v, n.Else)
	case *BetweenExpr:
		Walk(v, n.Expr)
		Walk(v, n.Lower)
		Walk(v, n.Upper)
	case *InExpr:
		Walk(v, n.Expr)
		for _, expr := range n.List {
			Walk(v, expr)
		}
		Walk(v, n.Subquery)
	case *LikeExpr:
		Walk(v, n.Expr)
		Walk(v, n.Pattern)
	case *IsNullExpr:
		Walk(v, n.Expr)
	case *ExistsExpr:
		Walk(v, n.Subquery)
	case *SubqueryExpr:
		Walk(v, n.Select)
	}

	v.Visit(nil)
}

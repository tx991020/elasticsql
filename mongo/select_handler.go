package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"strings"

	"github.com/tidwall/gjson"
	"github.com/xwb1989/sqlparser"
)

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (dsl string, table string, err error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", "", err
	}
	marshal, _ := json.Marshal(stmt)
	fmt.Println(77, string(marshal))

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err = handleSelect(stmt.(*sqlparser.Select))

	}

	if err != nil {
		return "", "", err
	}

	return dsl, table, nil
}

func handleSelectWhere(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, *SqlContext, error) {
	if expr == nil {
		return "", nil, errors.New("error expression cannot be nil here")
	}

	switch (*expr).(type) {

	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(expr, topLevel, parent)

	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(expr, topLevel, parent)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(expr, topLevel, parent)

	case *sqlparser.IsExpr:
		return "", nil, errors.New("is expression currently not supported")
	case *sqlparser.RangeCond:
		// between a and b
		// the meaning is equal to range query
		rangeCond := (*expr).(*sqlparser.RangeCond)
		colName, ok := rangeCond.Left.(*sqlparser.ColName)

		if !ok {
			return "", nil, errors.New("range column name missing")
		}

		colNameStr := sqlparser.String(colName)
		fromStr := sqlparser.String(rangeCond.From)
		fromStr = strings.ReplaceAll(fromStr, `'`, `"`)
		toStr := sqlparser.String(rangeCond.To)
		toStr = strings.ReplaceAll(toStr, `'`, `"`)

		resultStr := fmt.Sprintf(`{ $and : [{"%v" : { "$gte" : %v }}, {"%v" : { "$lte" : %v }}] }`, colNameStr, fromStr, colNameStr, toStr)

		if topLevel {

		}

		return resultStr, &SqlContext{}, nil

	case *sqlparser.ParenExpr:
		parentBoolExpr := (*expr).(*sqlparser.ParenExpr)
		boolExpr := parentBoolExpr.Expr

		// if paren is the top level, bool must is needed
		var isThisTopLevel = false
		if topLevel {
			isThisTopLevel = true
		}
		return handleSelectWhere(&boolExpr, isThisTopLevel, parent)
	case *sqlparser.NotExpr:
		return "", nil, errors.New("not expression currently not supported")
	case *sqlparser.FuncExpr:

	}

	return "", nil, errors.New(" logically cannot reached here")
}

func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {

	// Handle where
	// top level node pass in an empty interface
	// to tell the children this is root
	// is there any better way?
	var rootParent sqlparser.Expr
	var defaultQueryMapStr = `{}`
	var queryMapStr string
	needJoin := false
	table1 := ""
	if queryMapStr == "" {
		queryMapStr = defaultQueryMapStr
	}
	resultMap := map[string]interface{}{}

	aggFlag := false
	var aggStr string
	var colStr string

	//只支持2表单个字段eq
	if len(sel.From) > 0 {
		if checkNeedJoin(sel) {
			needJoin = true
			join, err := parserFromJoin(sel)
			if err != nil {
				return "", "", err
			}

			resultMap["$lookup"] = generateInternalLookup(join.Left, join.Right, join.LCol, join.RCol)

			project := map[string]interface{}{}
			project["_id"] = "NumberInt(0)"
			for _, expr := range sel.SelectExprs {
				s := sqlparser.String(expr)
				project[s] = fmt.Sprintf("$%s", s)
			}
			resultMap["$project"] = project

		}
		split := strings.Split(sqlparser.String(sel.From), " ")
		if len(split) > 0 {
			table1 = split[0]
		}

	}
	if len(sel.GroupBy) > 0 || checkNeedAgg(sel.SelectExprs) {

		aggFlag = true
		aggStr, colStr, err = buildAggs(sel)
		if err != nil {
			//aggStr = ""
			fmt.Println("agg err", err)
			return "", "", err
		}
		resultMap["$project"] = colStr
		resultMap["$group"] = aggStr

	}

	// use may not pass where clauses
	if sel.Where != nil {
		fmt.Println("where", sqlparser.String(sel.Where))
		queryMapStr, _, err = handleSelectWhere(&sel.Where.Expr, true, &rootParent)
		if err != nil {
			return "", "", err
		}
	}

	esType = sqlparser.String(sel.From)
	queryFrom, querySize := "0", "10"

	if sel.Having != nil {

		fmt.Println("having", sqlparser.String(sel.Having))
	}

	// Handle limit
	if sel.Limit != nil {

		if sel.Limit.Offset != nil {
			queryFrom = sqlparser.String(sel.Limit.Offset)

		}
		querySize = sqlparser.String(sel.Limit.Rowcount)

	}
	fmt.Println("from,size", queryFrom, querySize)

	swithOrder := func(direction string) (s int) {
		switch strings.ToUpper(direction) {
		case "ASC":
			s = 1
		case "DESC":
			s = -1
		}
		return
	}
	// Handle order by
	// when executating aggregations, order by is useless
	orderByMap := map[string]interface{}{}

	if aggFlag == false {
		for _, orderByExpr := range sel.OrderBy {
			key := sqlparser.String(orderByExpr.Expr)
			orderByMap[key] = swithOrder(orderByExpr.Direction)

		}
	}

	fmt.Println("query", queryMapStr)
	fmt.Println("aggStr", aggStr)

	if len(orderByMap) == 0 {
		orderByMap["_id"] = -1
	}
	resultMap["$match"] = queryMapStr
	var keySlice = []string{"$match", "$group", "$lookup", "$project"}
	var resultArr []string
	for _, mapKey := range keySlice {
		if val, ok := resultMap[mapKey]; ok {
			resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, mapKey, val))
		}
	}
	fmt.Println("yyy", resultArr)
	dsl = "{" + strings.Join(resultArr, ",") + "}"
	marshal, _ := json.Marshal(orderByMap)

	if len(aggStr) > 0 || needJoin {

		dsl = fmt.Sprintf(`db.%s.aggregate([%v]).sort(%s).skip(%s).limit(%s)`, table1, dsl, string(marshal), queryFrom, querySize)

	} else {
		fmt.Println("col", resultMap)

		dsl = fmt.Sprintf(`db.%s.find(%v).sort(%v).skip(%s).limit(%s)`, table1, dsl, string(marshal), queryFrom, querySize)
	}

	return dsl, esType, nil
}

func handleSelectWhereOrExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, *SqlContext, error) {
	orExpr := (*expr).(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right

	leftStr, sql, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", nil, err
	}

	rightStr, sql, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", sql, err
	}

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	// not toplevel
	// if the parent node is also or node, then merge the query param
	if _, ok := (*parent).(*sqlparser.OrExpr); ok {
		return resultStr, sql, nil
	}

	return fmt.Sprintf(`{"$or" : [%v]}`, resultStr), sql, nil
}

type SqlContext struct {
	Left    string
	LeftAs  string
	Right   string
	RightAs string
	Match   string
}

func handleSelectWhereAndExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, *SqlContext, error) {

	andExpr := (*expr).(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftStr, sql, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", nil, err
	}
	rightStr, sql, err := handleSelectWhere(&rightExpr, false, expr)

	if err != nil {
		return "", nil, err
	}

	// not toplevel
	// if the parent node is also and, then the result can be merged

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	if _, ok := (*parent).(*sqlparser.AndExpr); ok {
		return resultStr, sql, nil
	}

	return fmt.Sprintf(`{"$and" : [%v]}`, resultStr), sql, nil
}

func handleSelectWhereComparisonExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, *SqlContext, error) {
	comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

	if !ok {
		return "", nil, errors.New(" invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	rightStr, _, err := buildComparisonExprRightStr(comparisonExpr.Right)
	//rightStr = strings.ReplaceAll(rightStr, `'`, `"`)
	if err != nil {
		return "", nil, err
	}

	resultStr := ""

	switch comparisonExpr.Operator {
	case ">=":

		resultStr = fmt.Sprintf(`{  %v : { "$gte" : %v } }`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{  %v : { "$lte" : %v } }`, colNameStr, rightStr)
	case "=":
		re := regexp.MustCompile(`^\d+$`)
		matchString := re.MatchString(rightStr)

		if matchString {
			rightStr = fmt.Sprintf("NumberLong(%s)", rightStr)

		}
		resultStr = fmt.Sprintf(`{ "%v" : %v }`, colNameStr, rightStr)

	case ">":
		resultStr = fmt.Sprintf(`{  %v : { "$gt" : %v } }`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{  %v : { "$lt" : %v } }`, colNameStr, rightStr)
	case "!=":

		resultStr = fmt.Sprintf(`{  %v : { "$ne" : %v } }`, colNameStr, rightStr)

	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		fmt.Println("xxx in", colNameStr, rightStr)
		resultStr = fmt.Sprintf(`{  "%v" : { "$in" : [%v] } }`, colNameStr, rightStr)
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{  "%v" : { "$nin" : %v } }`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		//{  "$regex" : "^start.*$" }
		resultStr = fmt.Sprintf(`{ "%v" : "^%v.*$" }`, "$regex", rightStr)

	case "not like":
		//"$not": /^start.*$/
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{ "%v" : "/^%v.*$/" }`, "$not", rightStr)

	}

	// the root node need to have bool and must
	if topLevel {
		//resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}

	return resultStr, &SqlContext{}, nil
}

func buildComparisonExprRightStr(expr sqlparser.Expr) (string, bool, error) {
	var rightStr string
	var err error
	var missingCheck = false
	switch expr.(type) {
	case *sqlparser.SQLVal:
		rightStr = sqlparser.String(expr)
	case *sqlparser.GroupConcatExpr:
		fmt.Println(44444)
		return "", missingCheck, errors.New("elasticsql: group_concat not supported")
	case *sqlparser.FuncExpr:
		fmt.Println(2222)
		// parse nested
		funcExpr := expr.(*sqlparser.FuncExpr)
		rightStr, err = buildNestedFuncStrValue(funcExpr)
		if err != nil {
			return "", missingCheck, err
		}
	case *sqlparser.ColName:
		fmt.Println(1111)
		rightStr = sqlparser.String(expr)

		return rightStr, missingCheck, nil
	case sqlparser.ValTuple:
		fmt.Println(6666)
		rightStr = sqlparser.String(expr)
	case sqlparser.TableExpr:
		fmt.Println("table expr")

	default:
		// cannot reach here
	}
	return rightStr, missingCheck, err
}

func buildNestedFuncStrValue(nestedFunc *sqlparser.FuncExpr) (string, error) {
	return "", errors.New("elasticsql: unsupported function" + nestedFunc.Name.String())
}

// if the where is empty, need to check whether to agg or not
func checkNeedAgg(sqlSelect sqlparser.SelectExprs) bool {
	for _, v := range sqlSelect {
		expr, ok := v.(*sqlparser.AliasedExpr)
		if !ok {
			// no need to handle, star expression * just skip is ok
			continue
		}

		if _, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
			return true
		}
	}
	return false
}

func handlerSelectHaving() {

}

func checkNeedJoin(sel *sqlparser.Select) bool {

	if _, ok := (sel.From[0]).(*sqlparser.JoinTableExpr); ok {
		return true
	}
	return false
}

type JoinCon struct {
	Left    string
	LeftAs  string
	Right   string
	RightAs string
	LCol    string
	RCol    string
}

func parserFromJoin(sel *sqlparser.Select) (join *JoinCon, err error) {

	expr := sel.From[0]

	switch (sel.From[0]).(type) {
	case *sqlparser.JoinTableExpr:
		joinExpr := (expr).(*sqlparser.JoinTableExpr)
		marshal, _ := json.Marshal(joinExpr)
		join = &JoinCon{
			Left:    gjson.GetBytes(marshal, "LeftExpr.Expr.Name").String(),
			LeftAs:  gjson.GetBytes(marshal, "LeftExpr.As").String(),
			Right:   gjson.GetBytes(marshal, "RightExpr.Expr.Name").String(),
			RightAs: gjson.GetBytes(marshal, "RightExpr.As").String(),
			LCol:    gjson.GetBytes(marshal, "Condition.On.Left.Name").String(),
			RCol:    gjson.GetBytes(marshal, "Condition.On.Right.Name").String(),
		}

		return join, nil

	}
	return join, errors.New("parse join err")

}

package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

func handleSelectWhere(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	if expr == nil {
		return "", errors.New("elasticsql: error expression cannot be nil here")
	}

	switch e := (*expr).(type) {

	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(expr, topLevel, parent)

	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(expr, topLevel, parent)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(expr, topLevel, parent)

	case *sqlparser.IsExpr:
		return "", errors.New("elasticsql: is expression currently not supported")
	case *sqlparser.RangeCond:
		// between a and b
		// the meaning is equal to range query
		rangeCond := (*expr).(*sqlparser.RangeCond)
		colName, ok := rangeCond.Left.(*sqlparser.ColName)

		if !ok {
			return "", errors.New("elasticsql: range column name missing")
		}

		colNameStr := sqlparser.String(colName)
		fromStr := strings.Trim(sqlparser.String(rangeCond.From), `'`)
		toStr := strings.Trim(sqlparser.String(rangeCond.To), `'`)

		resultStr := fmt.Sprintf(`{ $and : [{"%v" : { $gte : %v }}, {"%v" : { $lte : %v }}] }`, colNameStr, fromStr, colNameStr, toStr)

		if topLevel {
			//resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
		}

		return resultStr, nil

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
		return "", errors.New("elasticsql: not expression currently not supported")
	case *sqlparser.FuncExpr:
		switch e.Name.Lowered() {
		case "multi_match":
			params := e.Exprs
			if len(params) > 3 || len(params) < 2 {
				return "", errors.New("elasticsql: the multi_match must have 2 or 3 params, (query, fields and type) or (query, fields)")
			}

			var typ, query, fields string
			for i := 0; i < len(params); i++ {
				elem := strings.Replace(sqlparser.String(params[i]), "`", "", -1) // a = b
				kv := strings.Split(elem, "=")
				if len(kv) != 2 {
					return "", errors.New("elasticsql: the param should be query = xxx, field = yyy, type = zzz")
				}
				k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
				switch k {
				case "type":
					typ = strings.Replace(v, "'", "", -1)
				case "query":
					query = strings.Replace(v, "`", "", -1)
					query = strings.Replace(query, "'", "", -1)
				case "fields":
					fieldList := strings.Split(strings.TrimRight(strings.TrimLeft(v, "("), ")"), ",")
					for idx, field := range fieldList {
						fieldList[idx] = fmt.Sprintf(`"%v"`, strings.TrimSpace(field))
					}
					fields = strings.Join(fieldList, ",")
				default:
					return "", errors.New("elaticsql: unknow param for multi_match")
				}
			}
			if typ == "" {
				return fmt.Sprintf(`{"multi_match" : {"query" : "%v", "fields" : [%v]}}`, query, fields), nil
			}
			return fmt.Sprintf(`{"multi_match" : {"query" : "%v", "type" : "%v", "fields" : [%v]}}`, query, typ, fields), nil
		default:
			return "", errors.New("elaticsql: function in where not supported" + e.Name.Lowered())
		}
	}

	return "", errors.New("elaticsql: logically cannot reached here")
}

func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {

	// Handle where
	// top level node pass in an empty interface
	// to tell the children this is root
	// is there any better way?
	var rootParent sqlparser.Expr
	var defaultQueryMapStr = `{}`
	var queryMapStr string
	if sel.SelectExprs != nil {

	}

	// use may not pass where clauses
	if sel.Where != nil {
		queryMapStr, err = handleSelectWhere(&sel.Where.Expr, true, &rootParent)
		if err != nil {
			return "", "", err
		}
	}

	if queryMapStr == "" {
		queryMapStr = defaultQueryMapStr
	}

	//Handle from
	if len(sel.From) != 1 {
		fmt.Println("yyy", sqlparser.String(sel.From))
		return "", "", nil
	}
	esType = sqlparser.String(sel.From)
	esType = strings.Replace(esType, "`", "", -1)
	queryFrom, querySize := "0", "1"

	aggFlag := false
	// if the request is to aggregation
	// then set aggFlag to true, and querySize to 0
	// to not return any query result

	var aggStr string
	if len(sel.GroupBy) > 0 || checkNeedAgg(sel.SelectExprs) {
		aggFlag = true
		aggStr, err = buildAggs(sel)
		if err != nil {
			//aggStr = ""
			return "", "", err
		}
	}
	if sel.Having != nil {

	}


	// Handle limit
	if sel.Limit != nil {
		if sel.Limit.Offset != nil {
			queryFrom = sqlparser.String(sel.Limit.Offset)
		}
		querySize = sqlparser.String(sel.Limit.Rowcount)
	}
	fmt.Println("from,size", queryFrom, querySize)

	// Handle order by
	// when executating aggregations, order by is useless
	orderByArr := make([]string, 0)

	if aggFlag == false {
		for _, orderByExpr := range sel.OrderBy {
			orderByStr := fmt.Sprintf(`{"%v": "%v"}`, strings.Replace(sqlparser.String(orderByExpr.Expr), "`", "", -1), orderByExpr.Direction)
			orderByArr = append(orderByArr, orderByStr)
		}
	}

	var resultMap = map[string]interface{}{
		"$match": queryMapStr,
	}
	fmt.Println("query", queryMapStr)

	if len(aggStr) > 0 {
		resultMap["$group"] = aggStr
	}

	if len(orderByArr) > 0 {
		resultMap["$sort"] = fmt.Sprintf("[%v]", strings.Join(orderByArr, ","))
	}

	// keep the travesal in order, avoid unpredicted json
	var keySlice = []string{"$match", "sort", "aggregations"}
	var resultArr []string
	for _, mapKey := range keySlice {
		if val, ok := resultMap[mapKey]; ok {
			resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, mapKey, val))
		}
	}

	dsl = "{" + strings.Join(resultArr, ",") + "}"
	return dsl, esType, nil
}

func handleSelectWhereOrExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	orExpr := (*expr).(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right

	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}

	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
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
		return resultStr, nil
	}

	return fmt.Sprintf(`{"$or" : [%v]}`, resultStr), nil
}

func handleSelectWhereAndExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {

	andExpr := (*expr).(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
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
		return resultStr, nil
	}

	return fmt.Sprintf(`{"$and" : [%v]}`, resultStr), nil
}

func handleSelectWhereComparisonExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

	if !ok {
		return "", errors.New("elasticsql: invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	rightStr, _, err := buildComparisonExprRightStr(comparisonExpr.Right)
	if err != nil {
		return "", err
	}

	resultStr := ""

	switch comparisonExpr.Operator {
	case ">=":

		resultStr = fmt.Sprintf(`{  %v : { '$gte' : %v } }`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{  %v : { '$lte' : %v } }`, colNameStr, rightStr)
	case "=":

		resultStr = fmt.Sprintf(`{ '%v' : %v }`, colNameStr, rightStr)

	case ">":
		resultStr = fmt.Sprintf(`{  %v : { '$gt' : %v } }`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{  %v : { '$lt' : %v } }`, colNameStr, rightStr)
	case "!=":

		resultStr = fmt.Sprintf(`{  %v : { '$ne' : %v } }`, colNameStr, rightStr)

	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		fmt.Println("xxx in", colNameStr, rightStr)
		resultStr = fmt.Sprintf(`{  %v : { '$in' : [%v] } }`, colNameStr, rightStr)
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{  %v : { '$nin' : %v } }`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		//{  "$regex" : "^start.*$" }
		resultStr = fmt.Sprintf(`{ '%v' : '^%v.*$' }`, "$regex", rightStr)

	case "not like":
		//"$not": /^start.*$/
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{ '%v' : '/^%v.*$/' }`, "$not", rightStr)

	}

	// the root node need to have bool and must
	if topLevel {
		//resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}

	return resultStr, nil
}

func buildComparisonExprRightStr(expr sqlparser.Expr) (string, bool, error) {
	var rightStr string
	var err error
	var missingCheck = false
	switch expr.(type) {
	case *sqlparser.SQLVal:
		rightStr = sqlparser.String(expr)
		rightStr = strings.Trim(rightStr, `'`)
	case *sqlparser.GroupConcatExpr:
		return "", missingCheck, errors.New("elasticsql: group_concat not supported")
	case *sqlparser.FuncExpr:
		// parse nested
		funcExpr := expr.(*sqlparser.FuncExpr)
		rightStr, err = buildNestedFuncStrValue(funcExpr)
		if err != nil {
			return "", missingCheck, err
		}
	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			missingCheck = true
			return "", missingCheck, nil
		}
		fmt.Println("xxx", sqlparser.String(expr))
		return "", missingCheck, nil
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(expr)
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

		//TODO more precise
		if _, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
			return true
		}
	}
	return false
}

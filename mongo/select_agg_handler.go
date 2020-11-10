package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// msi stands for map[string]interface{}
type msi map[string]interface{}

func handleFuncInSelectAgg(funcExprArr []*sqlparser.FuncExpr) msi {

	var innerAggMap = make(msi)
	for _, v := range funcExprArr {
		//func expressions will use the same parent bucket

		aggName := strings.ToUpper(v.Name.String()) + `(` + sqlparser.String(v.Exprs) + `)`
		fmt.Println("group func", v.Name.String(), aggName)
		switch v.Name.Lowered() {

		case "count":
			//count need to distinguish * and normal field name

			innerAggMap["COUNT(*)"] = map[string]interface{}{"$sum": 1}

		default:
			// support min/avg/max/stats
			// extended_stats/percentiles
			innerAggMap[aggName] = map[string]interface{}{fmt.Sprintf("$%s", v.Name.Lowered()): fmt.Sprintf("$%s", sqlparser.String(v.Exprs))}
		}

	}

	return innerAggMap

}

func handleGroupByColName(colName *sqlparser.ColName) string {

	return fmt.Sprintf("$%s", colName.Name.String())
}

func handleGroupByFuncExprRange(funcExpr *sqlparser.FuncExpr) (msi, error) {
	if len(funcExpr.Exprs) < 3 {
		return nil, errors.New("elasticsql: length of function range params must be > 3")
	}

	var innerMap = make(msi)
	rangeMapList := make([]msi, len(funcExpr.Exprs)-2)

	for i := 1; i < len(funcExpr.Exprs)-1; i++ {
		valFrom := sqlparser.String(funcExpr.Exprs[i])
		valTo := sqlparser.String(funcExpr.Exprs[i+1])
		rangeMapList[i-1] = msi{
			"from": valFrom,
			"to":   valTo,
		}
	}
	innerMap[funcExpr.Name.String()] = msi{
		"field":  sqlparser.String(funcExpr.Exprs[0]),
		"ranges": rangeMapList,
	}

	return innerMap, nil
}

func handleGroupByFuncExprDateRange(funcExpr *sqlparser.FuncExpr) (msi, error) {
	var innerMap msi
	var (
		field        string
		format       = "yyyy-MM-dd HH:mm:ss"
		rangeList    = []string{}
		rangeMapList = []msi{}
	)

	for _, expr := range funcExpr.Exprs {
		nonStarExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.New("elasticsql: unsupported star expression in function date_range")
		}

		switch item := nonStarExpr.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			colName := sqlparser.String(item.Left)
			equalVal := sqlparser.String(item.Right.(*sqlparser.SQLVal))
			//fmt.Printf("%#v", sqlparser.String(item.Right))
			equalVal = strings.Trim(equalVal, `'`)

			switch colName {
			case "field":
				field = equalVal
			case "format":
				format = equalVal
			default:
				return nil, errors.New("elasticsql: unsupported column name " + colName)
			}
		case *sqlparser.SQLVal:
			skippedString := strings.Trim(sqlparser.String(item), "`")
			rangeList = append(rangeList, skippedString)
		default:
			return nil, errors.New("elasticsql: unsupported expression " + sqlparser.String(expr))
		}
	}

	if len(field) == 0 {
		return nil, errors.New("elasticsql: lack field of date_range")
	}

	for i := 0; i < len(rangeList)-1; i++ {
		tmpMap := msi{
			"from": strings.Trim(rangeList[i], `'`),
			"to":   strings.Trim(rangeList[i+1], `'`),
		}
		rangeMapList = append(rangeMapList, tmpMap)
	}

	innerMap = msi{
		"date_range": msi{
			"field":  field,
			"ranges": rangeMapList,
			"format": format,
		},
	}

	return innerMap, nil
}

func handleGroupByFuncExpr(funcExpr *sqlparser.FuncExpr, child msi) (msi, error) {

	var innerMap msi
	var err error

	switch funcExpr.Name.Lowered() {

	case "range":
		innerMap, err = handleGroupByFuncExprRange(funcExpr)
	case "date_range":
		innerMap, err = handleGroupByFuncExprDateRange(funcExpr)
	default:
		return nil, errors.New("elasticsql: unsupported group by functions" + sqlparser.String(funcExpr))
	}

	if err != nil {
		return nil, err
	}

	if len(child) > 0 && innerMap != nil {
		innerMap["aggregations"] = child
	}

	stripedFuncExpr := sqlparser.String(funcExpr)
	stripedFuncExpr = strings.Replace(stripedFuncExpr, " ", "", -1)
	stripedFuncExpr = strings.Replace(stripedFuncExpr, "'", "", -1)
	return msi{stripedFuncExpr: innerMap}, nil
}

func handleGroupByAgg(groupBy sqlparser.GroupBy, innerMap msi) (msi, error) {

	var aggMap = make(msi)

	var child = innerMap
	tem := make(map[string]interface{})
	for i := len(groupBy) - 1; i >= 0; i-- {
		v := groupBy[i]

		switch item := v.(type) {
		case *sqlparser.ColName:

			tem[item.Name.String()] = handleGroupByColName(item)

		case *sqlparser.FuncExpr:
			currentMap, err := handleGroupByFuncExpr(item, child)
			if err != nil {
				return nil, err
			}
			child = currentMap
		}
	}
	if len(tem) > 0 {
		child["_id"] = tem
	}

	aggMap = child

	return aggMap, nil
}

func buildAggs(sel *sqlparser.Select) (string, error) {

	funcExprArr, _, funcErr := extractFuncAndColFromSelect(sel.SelectExprs)
	innerAggMap := handleFuncInSelectAgg(funcExprArr)
	if funcErr != nil {
	}

	aggMap, err := handleGroupByAgg(sel.GroupBy, innerAggMap)
	if err != nil {
		return "", err
	}

	mapJSON, _ := json.Marshal(aggMap)
	fmt.Println("agg func", string(mapJSON))
	return string(mapJSON), nil
}

// extract func expressions from select exprs
func extractFuncAndColFromSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName, error) {
	var colArr []*sqlparser.ColName
	var funcArr []*sqlparser.FuncExpr
	for _, v := range sqlSelect {
		// non star expressioin means column name
		// or some aggregation functions
		expr, ok := v.(*sqlparser.AliasedExpr)
		if !ok {
			// no need to handle, star expression * just skip is ok
			continue
		}

		// NonStarExpr start
		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			funcArr = append(funcArr, funcExpr)

		case *sqlparser.ColName:
			continue
		default:
			//ignore
		}

		//starExpression like *, table.* should be ignored
		//'cause it is meaningless to set fields in elasticsearch aggs
	}
	return funcArr, colArr, nil
}

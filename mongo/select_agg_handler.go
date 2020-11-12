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

		}
	}
	if len(tem) > 0 {
		child["_id"] = tem
	}

	aggMap = child

	return aggMap, nil
}

func buildAggs(sel *sqlparser.Select) (string, string,error) {

	funcExprArr, _, colMap,funcErr := extractFuncAndColFromSelect(sel.SelectExprs)
	innerAggMap := handleFuncInSelectAgg(funcExprArr)
	if funcErr != nil {
	}

	aggMap, err := handleGroupByAgg(sel.GroupBy, innerAggMap)
	if err != nil {
		return "","", err
	}

	mapJSON, _ := json.Marshal(aggMap)
	mapJSON1, _ := json.Marshal(colMap)
	fmt.Println("agg func", string(mapJSON))
	fmt.Println("cols", string(mapJSON))
	return string(mapJSON),string(mapJSON1), nil
}

// extract func expressions from select exprs
func extractFuncAndColFromSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName,map[string]interface{}, error) {
	var colArr []*sqlparser.ColName
	var funcArr []*sqlparser.FuncExpr
	colMap :=make(map[string]interface{})
	colMap["_id"]=0
	for _, v := range sqlSelect {

		expr, ok := v.(*sqlparser.AliasedExpr)
		if !ok {
			// no need to handle, star expression * just skip is ok
			continue
		}

		// NonStarExpr start
		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			aggName := strings.ToUpper(funcExpr.Name.String()) + `(` + sqlparser.String(funcExpr.Exprs) + `)`
			colMap[aggName]=fmt.Sprintf("$%s",aggName)
			funcArr = append(funcArr, funcExpr)

		case *sqlparser.ColName:
			colExpr := expr.Expr.(*sqlparser.ColName)
			colMap[colExpr.Name.String()]=fmt.Sprintf("$_id.%s",colExpr.Name.String())

		default:

		}

	}
	return funcArr, colArr,colMap, nil
}



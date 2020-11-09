package main

import (
	"fmt"
	"strings"

	"github.com/tx991020/elasticsql/mongo"
	"github.com/xwb1989/sqlparser"
)

func main() {
	sql := `SELECT * from t  where age = 1 and nem ="haha"`
	convert, table, err := mongo.Convert(sql)
	if err != nil {
		panic(err)
		return
	}
	fmt.Println(convert,table)
}

func todo()  {
	sql := `SELECT u.c1,u.c2,h.c2 FROM user.csv as u LEFT JOIN hist.csv as h ON(u.c1=h.c1)`
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(err)
		return
	}
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err := handleSelect(stmt.(*sqlparser.Select))
		fmt.Println(111,dsl, table, err)
	}
}

func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {

	fmt.Println(len(sel.From))
	esType = strings.Replace(esType, "`", "", -1)
	return
}
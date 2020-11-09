package main

import (
	"fmt"

	"github.com/tx991020/elasticsql"
)

func main() {
	sql := `SELECT name from t`
	convert, table, err := elasticsql.Convert(sql)
	if err != nil {
		panic(err)
		return
	}
	fmt.Println(convert,table)
}

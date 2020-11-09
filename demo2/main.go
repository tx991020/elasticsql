package main

import (
	"fmt"

	"github.com/tx991020/elasticsql"
)

func main() {
	sql := `SELECT * from t  where age = 1 and nem ="haha"`
	convert, table, err := elasticsql.Convert(sql)
	if err != nil {
		panic(err)
		return
	}
	fmt.Println(convert,table)
}

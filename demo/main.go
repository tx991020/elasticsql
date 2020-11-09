package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tx991020/elasticsql/mongo"
	"github.com/xwb1989/sqlparser"
)

func main() {
	sql := `select * from aaa
where a=1 and x = '三个男人'
and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800'
and process_id > 1 order by id desc limit 100,10`
	sql1 :=`select x.c1,y.c2,z.c3 from x, y, z where x.c1 = y.c1 and x.c1 = z.c1`
	fmt.Println(sql,sql1)
	convert, table, err := mongo.Convert(sql1)

	if err != nil {
		panic(err)
		return
	}
	fmt.Println(convert,table)

}

func todo(sql string)  {

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
	marshal, _ := json.Marshal(sel)
	fmt.Println(string(marshal))
	esType = strings.Replace(esType, "`", "", -1)
	return
}
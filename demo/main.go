package main

import (
	"fmt"

	"github.com/tx991020/elasticsql/mongo"
)

func main() {
	sql1 := `select * from aaa
where a=1 and x = '三个男人'
and create_time between "2015-01-01T00:00:00+0800" and "2016-01-01T00:00:00+0800"
and process_id > 1 order by id desc,ctime asc limit 100,10`
//	sql1 :=`select t1.column1, t2.column2,t1.e,t2.e from my_table as t1 inner join my_table2 as t2 on t1.c1 = t2.c1 `
//	sql1 := `select t1, t2 from q where c in ("1","2","3")`

	//fmt.Println(sql,sql1)
	convert, table, err := mongo.Convert(sql1)

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("sql",convert)
	fmt.Println(table)

}

//func todo1(sql string)  {
//
//	stmt, err := sqlparser.Parse(sql)
//	if err != nil {
//		panic(err)
//		return
//	}
//	switch stmt.(type) {
//	case *sqlparser.Select:
//		dsl, table, err := handleSelect(stmt.(*sqlparser.Select))
//		fmt.Println(111,dsl, table, err)
//	}
//}
//
//func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {
//	marshal, _ := json.Marshal(sel)
//	fmt.Println(string(marshal))
//	esType = strings.Replace(esType, "`", "", -1)
//	return
//}
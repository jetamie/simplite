package simplite

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"strings"
)

type SqlAdaptor interface {
	CreatTable(sqlStr string)
	Insert(table string,data []map[string]interface{})
	Update(table string,data map[string]interface{},where map[string]interface{})
	Delete(table string,where map[string]string)
	Query(sqlStr string) []map[string]interface{}
}

type sqlEngine struct {
	db *sql.DB
}

func NewSqlEngine(files string) *sqlEngine {
	db,err := sql.Open("sqlite3", files)
	if err != nil {
		panic(err)
	}
	return &sqlEngine{
		db:db,
	}
}

//执行方法
func (en *sqlEngine) prepare(prepare string,line []interface{}) {
	stmt,err := en.db.Prepare(prepare)
	if err != nil {
		panic(err)
	}

	res,err := stmt.Exec(line...)
	if err != nil {
		panic(err)
	}

	row,err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	log.Fatalf("%v",row)
}

func (en *sqlEngine)CreatTable(sqlStr string)  {
	res,err := en.db.Exec(sqlStr)
	if err != nil {
		panic(err)
	}
	log.Printf("%v",res)
}

func (en *sqlEngine) Insert(table string,data []map[string]interface{}) {
	if data == nil || table == "" {
		panic("参数不能为空")
	}

	var (
		key string//字段
		val string//对应模式
		mode string//组合语句
	)
	for k,_ := range data[0] {
		key += k + ","
		val += "?,"
	}
	mode = "(" + strings.TrimRight(key, ",") + ") values (" + strings.TrimRight(val, ",") + ")"
	prepare := "INSERT INTO " + table + mode
	println(prepare)
	stmt,err := en.db.Prepare(prepare)
	if err != nil {
		panic(err)
	}
	for _,v := range data {
		var tmp []interface{}
		for _,r := range v {
			tmp = append(tmp,r)
		}
		fmt.Println(tmp...)
		res,err :=stmt.Exec(tmp...)
		if err != nil {
			panic(err)
		}
		log.Println(res.LastInsertId())
	}
}

func (en *sqlEngine) Update(table string,data map[string]interface{},where map[string]interface{}) {
	if where == nil || table == "" || data == nil{
		panic("参数不能为空")
	}

	var (
		set string
		wheres string
	)

	//数据拼接
	var line []interface{}
	for k,v := range data {
		set = set + k + "=?,"
		line = append(line,v)
	}
	set = strings.TrimRight(set,",")
	for k,v := range where {
		wheres = wheres + k + "=? and "
		line = append(line,v)
	}
	wheres = strings.TrimRight(wheres, " and ")

	prepare := "update %s set %s where %s"
	prepare = fmt.Sprintf(prepare,table,set,wheres)
	println(prepare)
	en.prepare(prepare,line)
}

func (en *sqlEngine) Delete(table string,where map[string]string) {
	if where == nil || table == "" {
		panic("参数不能为空")
	}

	var (
		wheres string
		line []interface{}
	)
	for k,v := range where {
		wheres = wheres + k + "=? and "
		line = append(line,v)
	}
	wheres = strings.TrimRight(wheres, " and ")

	prepare := "delete from %s where %s"
	prepare = fmt.Sprintf(prepare,table,wheres)
	//执行
	en.prepare(prepare,line)
}

func (en *sqlEngine) Query(sqlStr string) []map[string]interface{} {
	res,err := en.db.Query(sqlStr)

	if err != nil {
		panic(err)
	}
	defer res.Close()
	col,_ := res.Columns()
	count := len(col)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)

	for i := range values {
		scanArgs[i] = &values[i]
	}
	var ret  []map[string]interface{}
	for res.Next() {
		err := res.Scan(scanArgs...)
		if err != nil {
			log.Fatalf("Sacn data error: %s", err.Error())
			continue
		}
		entry := make(map[string]interface{})
		for i, col := range col {
			v := values[i]

			b, ok := v.([]byte)
			if ok {
				entry[col] = string(b)
			} else {
				entry[col] = v
			}
			ret = append(ret, entry)
		}
	}
	return ret
}
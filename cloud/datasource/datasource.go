package datasource

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
)

var Db *gorm.DB

func init() {
	path := strings.Join([]string{"", ":", "", "@(", "", ":", "", ")/", "", "?charset=utf8&parseTime=true&loc=Local"}, "")
	var err error
	Db, err = gorm.Open("mysql", path)
	if err != nil {
		panic(err)
	}
	Db.SingularTable(true)
	Db.DB().SetConnMaxLifetime(1 * time.Second)
	Db.DB().SetMaxIdleConns(20)   //最大打开的连接数
	Db.DB().SetMaxOpenConns(2000) //设置最大闲置个数
	Db.SingularTable(true)        //表生成结尾不带s
	// 启用Logger，显示详细日志
	Db.LogMode(true)
}

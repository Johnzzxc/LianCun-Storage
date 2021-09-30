package model

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
	"time"
)

func (CloudDict) TableName() string {
	return "cloud_dict"
}

func CloudDictDataFactory() *CloudDict {
	return &CloudDict{DB: datasource.Db}
}


// CloudDict [...]
type CloudDict struct {
	*gorm.DB   `gorm:"-" json:"-"`
	ID         int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"` // 数据主体 主键
	CreateTime time.Time `gorm:"column:create_time;type:timestamp" json:"create_time"`
	UpdateTime time.Time `gorm:"column:update_time;type:timestamp" json:"update_time"`
	State      int16     `gorm:"column:state;type:int(2)" json:"state"`  // 1 正常
	Name       string    `gorm:"column:name;type:varchar(255)" json:"name"`   //描述
	Type       string    `gorm:"column:type;type:varchar(255)" json:"type"`   //key
	Value      string    `gorm:"column:value;type:varchar(255)" json:"value"` // value
}

//返回cloud_dict 的value信息
func (u *CloudDict) GetTypeValue(Type string) (string, error) {
	var value []string
	//增加判断，审核通过的数据主体简称
	err := u.Table(u.TableName()).Where("state=1 and type = ?", Type).Pluck("value", &value).Error
	if err != nil {
		return "", err
	} else {
		fmt.Println(value[0])
		return value[0], nil
	}
}

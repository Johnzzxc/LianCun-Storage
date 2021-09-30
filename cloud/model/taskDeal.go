package model

import (
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
)

func (CloudTaskDeal) TableName() string {
	return "cloud_task_deal"
}

func TaskDealFactory() *CloudTaskDeal {
	return &CloudTaskDeal{DB: datasource.Db}
}

// CloudTaskDeal [...]
type CloudTaskDeal struct {
	*gorm.DB    `gorm:"-" json:"-"`
	CloudFileID int64  `gorm:"column:cloud_file_id;type:bigint(20)" json:"cloud_file_id"`
	CloudDealID int64  `gorm:"column:cloud_deal_id;type:bigint(20)" json:"cloud_deal_id"`
	TaskID      string `gorm:"column:task_id;type:varchar(64)" json:"task_id"`
}

//addDeal 根据文件id查询对应的deal的条数
func (u *CloudTaskDeal) CountDeal(fileId int64) (int64, error) {
	var count int64
	err := u.Table(u.TableName()).Where("cloud_file_id = ?", fileId).Count(&count).Error
	if err != nil {
		return -1, err
	} else {
		return count, nil
	}
}

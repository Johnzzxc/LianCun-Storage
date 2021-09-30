package model

import (
	"errors"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
	"strconv"
	"time"
)

func (CloudTaskRetrieve) TableName() string {
	return "cloud_task_retrieve"
}

func TaskRetrieveFactory() *CloudTaskRetrieve {
	return &CloudTaskRetrieve{DB: datasource.Db}
}

// CloudTaskRetrieve [...]
type CloudTaskRetrieve struct {
	*gorm.DB    `gorm:"-" json:"-"`
	ID          int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"`
	CreateTime  time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	UpdateTime  time.Time `gorm:"column:update_time;autoCreateTime" json:"update_time"`
	MinerID     int64     `gorm:"column:miner_id;type:bigint(20)" json:"miner_id"`
	CloudFileID int64     `gorm:"column:cloud_file_id;type:bigint(20)" json:"cloud_file_id"`
	CloudDealID int64     `gorm:"column:cloud_deal_id;type:bigint(20)" json:"cloud_deal_id"`
	TaskID      string    `gorm:"column:task_id;type:varchar(255)" json:"task_id"`
	State       int16     `gorm:"column:state;type:smallint(4)" json:"state"` // 9 done
}

//addRetrieve 在taskRetrieve表中插入一条数据 文件的id 供检索,状态9 为已完成
func (u *CloudTaskRetrieve) AddRetrieve(fileId int64, minerId int64,dealId int64) error {
	random, _ := uuid.NewRandom()
	timeStamp := time.Now().Unix()
	retrieveId := "retrieve-" + random.String() + "-" + strconv.FormatInt(timeStamp, 10)

	taskRetrieve := CloudTaskRetrieve{
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
		CloudFileID: fileId,
		TaskID:      retrieveId,
		MinerID:     minerId,
		State:       1,
		CloudDealID: dealId,
	}
	var count int64
	err := u.Table(u.TableName()).Where("cloud_file_id = ? AND state != ?", fileId, 9).Count(&count).Error
	if err != nil {
		return err
	} else {
		if count > 0 {
			return errors.New("任务执行中，请勿重复新增")
		} else {
			err := u.Table(u.TableName()).Create(&taskRetrieve).Error
			if err != nil {
				return err
			} else {
				return nil
			}
		}
	}

}

//GetRetrieve 根据
func (u *CloudTaskRetrieve) GetRetrieve(fileId int64) (CloudTaskRetrieve, error) {
	var cloudTaskRetrieve CloudTaskRetrieve
	err := u.Table(u.TableName()).Where("cloud_file_id = ?", fileId).Find(&cloudTaskRetrieve).Error
	if err != nil {
		return cloudTaskRetrieve, err
	} else {
		return cloudTaskRetrieve, nil
	}
}

package model

import (
	"errors"
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
	"math"
	"strconv"
	"time"
)

func (CloudFileDeal) TableName() string {
	return "cloud_file_deal"
}

func FileDealFactory() *CloudFileDeal {
	return &CloudFileDeal{DB: datasource.Db}
}

// CloudFileDeal [...]
type CloudFileDeal struct {
	*gorm.DB         `gorm:"-" json:"-"`
	ID               int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"`
	CreateTime       time.Time `gorm:"column:create_time;type:timestamp" json:"create_time"`
	UpdateTime       time.Time `gorm:"column:update_time;type:timestamp" json:"update_time"`
	FileID           int64     `gorm:"column:file_id;type:bigint(20)" json:"file_id"`                       // file表主键
	ProposalCid      string    `gorm:"column:proposal_cid;type:varchar(100)" json:"proposal_cid"`           // deal的cid
	MessageID        string    `gorm:"column:message_id;type:varchar(100)" json:"message_id"`               // 关联的messgaeid
	CloudMinerID     int64     `gorm:"column:cloud_miner_id;type:bigint(20)" json:"cloud_miner_id"`         // 关联的minerid
	Message          string    `gorm:"column:message;type:varchar(1000)" json:"message"`                    // 发送deal的返回消息
	Provider         string    `gorm:"column:Provider;type:varchar(64)" json:"provider"`                    // 服务端
	DealID           int       `gorm:"column:deal_id;type:int(64)" json:"deal_id"`                          // 发送deal的返回的主键
	Client           string    `gorm:"column:client;type:varchar(100)" json:"client"`                       // 客户端
	PieceSize        int64     `gorm:"column:piece_size;type:bigint(64)" json:"piece_size"`                 // 文件大小
	PieceCid         string    `gorm:"column:piece_cid;type:varchar(100)" json:"piece_cid"`                 // 文件的cid
	VerifiedDeal     []uint8   `gorm:"column:verified_deal;type:bit(1)" json:"verified_deal"`               // deal是否已验证
	EndDeal          int64     `gorm:"column:end_deal;type:bigint(32)" json:"end_deal"`                     // deal的结束高度
	StartDeal        int64     `gorm:"column:start_deal;type:bigint(32)" json:"start_deal"`                 // deal的开始高度
	Price            string    `gorm:"column:price;type:varchar(100)" json:"price"`                         // 价格
	MinerCollateral  string    `gorm:"column:miner_collateral;type:varchar(100)" json:"miner_collateral"`   // 矿工抵押
	ClientCollateral string    `gorm:"column:client_collateral;type:varchar(100)" json:"client_collateral"` // 客户端抵押
	State            int16     `gorm:"column:state;type:smallint(4)" json:"state"`                          // 状态
	StateName        string    `gorm:"column:state_name;type:varchar(255)" json:"state_name"`               // 状态描述
	RootID           string    `gorm:"column:root_id;type:varchar(100)" json:"root_id"`                     // 导入文件返回的rootid
	DealTime         string    `gorm:"column:deal_time;type:varchar(32)" json:"deal_time"`                  // deal执行时间
	TaskID           string    `gorm:"column:task_id;type:varchar(100)" json:"task_id"`                     // 任务id
	TotalCost        string    `gorm:"column:total_cost;type:varchar(64)" json:"total_cost"`                // Gas费
	DelFlag          []uint8   `gorm:"column:del_flag;type:bit(1)" json:"del_flag"`                         // 逻辑删除标识
}

//GetDealList 根据平文件id获取该文件的deal信息
func (u *CloudFileDeal) GetDealList(fileId int64) ([]CloudFileDeal, error) {
	var cloudFile []CloudFileDeal
	err := u.Table(u.TableName()).Where("file_id = ?", fileId).Find(&cloudFile).Error
	if err != nil {
		return nil, err
	} else {
		return cloudFile, nil
	}
}

//SpaceDate 根据起始高度计算起始时间
func (u *CloudFileDeal) SpaceDate() string {
	//创世时间
	//2020-08-25 06:00:00
	endTp, _ := time.ParseDuration(strconv.FormatInt(u.EndDeal*30, 10) + "s")
	startTp, _ := time.ParseDuration(strconv.FormatInt(u.StartDeal*30, 10) + "s")

	GenesisTime, _ := time.Parse("2006-01-02 15:04:05", "2020-08-25 06:00:00")
	startTime := GenesisTime.Add(startTp)
	endTime := GenesisTime.Add(endTp)
	days := strconv.FormatInt(int64(math.Floor(endTime.Sub(startTime).Hours()/24)), 10)

	startTF := startTime.Format("2006-01-02")
	endTF := endTime.Format("2006-01-02")

	return days + "天\n" + startTF + " ~ " + endTF
}

func (u *CloudFileDeal) GetMinerIdDealId(fileId int64) (int64,int64, error) {
	var cloudFile CloudFileDeal
	db := u.Table(u.TableName()).Where("file_id = ? AND state = 7", fileId).First(&cloudFile)
	err := db.Error
	if err != nil {
		if db.RowsAffected == 0 {
			return 0, 0, errors.New("该文件暂未上链")
		} else {
			return 0, 0, err
		}
	} else {
		return cloudFile.CloudMinerID, cloudFile.ID, nil
	}

}

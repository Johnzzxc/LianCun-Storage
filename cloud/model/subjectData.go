package model

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
	"time"
)

func (CloudSubjectData) TableName() string {
	return "cloud_subject_data"
}

func CloudSubjectDataFactory() *CloudSubjectData {
	return &CloudSubjectData{DB: datasource.Db}
}

// CloudSubjectData [...]
type CloudSubjectData struct {
	*gorm.DB                    `gorm:"-" json:"-"`
	ID                          int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"` // 数据主体 主键
	CreateTime                  time.Time `gorm:"column:create_time;type:timestamp" json:"create_time"`
	UpdateTime                  time.Time `gorm:"column:update_time;type:timestamp" json:"update_time"`
	DataType                    int16     `gorm:"column:data_type;type:smallint(4)" json:"data_type"`                                    // 1：A类主体，2：B类主体，3：C类主体
	FileLable                   string    `gorm:"column:file_lable;type:varchar(300)" json:"file_lable"`                                 // 数据类型标签
	DataApplyCapacity           int16     `gorm:"column:data_apply_capacity;type:smallint(4)" json:"data_apply_capacity"`                // 数据存储大小 1-100gb 2-500gb 3-1.5tb 4-5tb 5-5tb以上
	DataActualCapacity          float64   `gorm:"column:data_actual_capacity;type:decimal(32,0)" json:"data_actual_capacity"`            // 实际容量，
	CompayType                  int16     `gorm:"column:compay_type;type:smallint(4)" json:"compay_type"`                                // 公司类型  1：中国大陆，2：港澳台，3：海外
	CompayName                  string    `gorm:"column:compay_name;type:varchar(100)" json:"compay_name"`                               // 公司名称
	CompayWebsite               string    `gorm:"column:compay_website;type:varchar(500)" json:"compay_website"`                         // 公司官网
	CompaySocialURL             string    `gorm:"column:compay_social_url;type:varchar(500)" json:"compay_social_url"`                   // 公司社交媒体链接
	DataContactsName            string    `gorm:"column:data_contacts_name;type:varchar(50)" json:"data_contacts_name"`                  // 数据主体联系人姓名
	DataContactsMobile          string    `gorm:"column:data_contacts_mobile;type:varchar(32)" json:"data_contacts_mobile"`              // 数据主体联系人电话
	DataReferred                string    `gorm:"column:data_referred;type:varchar(32)" json:"data_referred"`                            // 数据主体简称
	ManagementModel             string    `gorm:"column:management_model;type:varchar(300)" json:"management_model"`                     // 经营模式
	TechnicalCapability         string    `gorm:"column:technical_capability;type:varchar(300)" json:"technical_capability"`             // 技术能力
	StorageStatus               string    `gorm:"column:storage_status;type:varchar(300)" json:"storage_status"`                         // 存储现状
	FilecoinUnderstand          string    `gorm:"column:filecoin_understand;type:varchar(300)" json:"filecoin_understand"`               // 对分布式存储的了解与贡献
	BusinessLicense             string    `gorm:"column:business_license;type:varchar(300)" json:"business_license"`                     // 公司营业执照
	GrantAuthorization          string    `gorm:"column:grant_authorization;type:varchar(300)" json:"grant_authorization"`               // 授权证明
	ProofIDentity               string    `gorm:"column:proof_identity;type:varchar(300)" json:"proof_identity"`                         // 身份证明
	ShareholderStructure        string    `gorm:"column:shareholder_structure;type:varchar(300)" json:"shareholder_structure"`           // 股东结构
	ShareholderProofIDentity    string    `gorm:"column:shareholder_proof_identity;type:varchar(300)" json:"shareholder_proof_identity"` // 股东身份证明
	AuditState                  int16     `gorm:"column:audit_state;type:smallint(4)" json:"audit_state"`                                // 审核状态 1：审核中，2：审核通过，3：退回
	AuditFeedback               string    `gorm:"column:audit_feedback;type:varchar(300)" json:"audit_feedback"`                         // 审核反馈
	WalletAddress               int64     `gorm:"column:wallet_address;type:bigint(20)" json:"wallet_address"`                           // 钱包地址ID
	PlatformID                  int64     `gorm:"column:platform_id;type:bigint(20)" json:"platform_id"`                                 // 平台用户ID
	ApplyType                   int16     `gorm:"column:apply_type;type:smallint(4)" json:"apply_type"`                                  // 申请类型，1：新申请，2：升级，3：扩容
	SubjectState                int16     `gorm:"column:subject_state;type:smallint(4)" json:"subject_state"`                            // 最新 升级\扩容审核状态 1：审核中，2：审核通过，3：退回
	DelFlag                     []uint8   `gorm:"column:del_flag;type:bit(1)" json:"del_flag"`                                           // 0：正常，1：删除
	BusinessLicenseURL          string    `gorm:"column:business_license_url;type:varchar(400)" json:"business_license_url"`
	GrantAuthorizationURL       string    `gorm:"column:grant_authorization_url;type:varchar(400)" json:"grant_authorization_url"`
	ProofIDentityURL            string    `gorm:"column:proof_identity_url;type:varchar(400)" json:"proof_identity_url"`
	ShareholderProofIDentityURL string    `gorm:"column:shareholder_proof_identity_url;type:varchar(400)" json:"shareholder_proof_identity_url"`
	UUID                        string    `gorm:"column:uuid;type:varchar(100)" json:"uuid"`
	PermState                   int16     `gorm:"column:perm_state;type:smallint(4)" json:"perm_state"` // 1-未分配 2-已分配
}

//GetSubjectID 根据bucketName即用户名称查询数据主体的id
func (u *CloudSubjectData) GetSubjectID(bucketName string) (int64, error) {
	var total []int64
	//增加判断，审核通过的数据主体简称
	err := u.Table(u.TableName()).Where("audit_state=2 and data_referred = ?", bucketName).Pluck("id", &total).Error
	if err != nil {
		return -1, err
	} else {
		fmt.Println(total[0])
		return total[0], nil
	}
}

//GetSubjectID 根据bucketName即用户名称查询数据主体的id
func (u *CloudSubjectData) CreateBucket(bucketName string,platformId int64) error {

	cloudFile := CloudSubjectData{
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
		PlatformID:  platformId,
		DataReferred: bucketName,
		AuditState: 2,
		SubjectState: 2,
		PermState: 2,
		ApplyType: 1,
	}
	err := u.Table(u.TableName()).Create(&cloudFile).Error

	if err != nil {
		return err
	} else {
		return nil
	}
}

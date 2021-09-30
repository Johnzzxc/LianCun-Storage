package model

import (
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
)

func (CloudPlatformInfo) TableName() string {
	return "cloud_platform_info"
}

func CloudPlatformInfoFactory() *CloudPlatformInfo {
	return &CloudPlatformInfo{DB: datasource.Db}
}

// CloudPlatformInfo [...]
type CloudPlatformInfo struct {
	*gorm.DB               `gorm:"-" json:"-"`
	AccessKeyID            string  `gorm:"column:access_key_id;type:varchar(64)" json:"access_key_id"`
	AccessKeySecret        string  `gorm:"column:access_key_secret;type:varchar(64)" json:"access_key_secret"`
	Name                   string  `gorm:"column:name;type:varchar(255)" json:"name"`
	Mobile                 string  `gorm:"column:mobile;type:varchar(32)" json:"mobile"`
	SaleID                 int64   `gorm:"column:sale_id;type:bigint(20)" json:"sale_id"`                  // 所属销售ID
	AuditFeedback          string  `gorm:"column:audit_feedback;type:varchar(300)" json:"audit_feedback"`  // 审核反馈
	CompayName             string  `gorm:"column:compay_name;type:varchar(100)" json:"compay_name"`        // 公司名称
	BankName               string  `gorm:"column:bank_name;type:varchar(100)" json:"bank_name"`            // 企业开户银行
	BankAccount            string  `gorm:"column:bank_account;type:varchar(100)" json:"bank_account"`      // 企业收款账号
	ContactsName           string  `gorm:"column:contacts_name;type:varchar(50)" json:"contacts_name"`     // 联系人
	ContactsMobile         string  `gorm:"column:contacts_mobile;type:varchar(32)" json:"contacts_mobile"` // 联系人电话
	ContactsEmail          string  `gorm:"column:contacts_email;type:varchar(100)" json:"contacts_email"`  // 联系人email
	Address                string  `gorm:"column:address;type:varchar(1000)" json:"address"`               // 寄件地址
	StateMessage           []uint8 `gorm:"column:state_message;type:bit(1)" json:"state_message"`          // 实名认证提醒，0：提醒，1：不提醒
	SubjectMessage         []uint8 `gorm:"column:subject_message;type:bit(1)" json:"subject_message"`      // 提交数据主体提醒，0：提醒，1：不提醒
	RealState              int16   `gorm:"column:real_state;type:smallint(4)" json:"real_state"`           // 实名认证状态 0-已实名 1-未实名 2-审核中，3-认证退回
	BusinessLicenseURL     string  `gorm:"column:business_license_url;type:varchar(400)" json:"business_license_url"`
	BusinessLicenseSaveURL string  `gorm:"column:business_license_save_url;type:varchar(255)" json:"business_license_save_url"`
	PassWordHash           string  `gorm:"column:pass_word_hash;type:varchar(100)" json:"pass_word_hash"`
	DelFlag                []uint8 `gorm:"column:del_flag;type:bit(1)" json:"del_flag"`
	Remarks                string  `gorm:"column:remarks;type:varchar(255)" json:"remarks"`
}

//GetPlatformBySecretKey 根据secretKey获取对应的平台id
func (u *CloudPlatformInfo) GetPlatformBySecretKey(secretKey string) (int64, error) {
	var total []int64
	err := u.Table(u.TableName()).Where("access_key_id = ?", secretKey).Pluck("id", &total).Error
	if err != nil {
		return -1, err
	} else {
		return total[0], nil
	}
}

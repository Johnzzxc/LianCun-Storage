package model

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/minio/minio/cloud/datasource"
	"strings"
	"time"
)

func (CloudFile) TableName() string {
	return "cloud_file"
}
func (CloudFileList) TableName() string {
	return "cloud_file"
}

func CloudFileFactory() *CloudFile {
	return &CloudFile{DB: datasource.Db}
}

func CloudFileListFactory() *CloudFileList {
	return &CloudFileList{DB: datasource.Db}
}

type CloudFilePrefixes struct {
	*gorm.DB `gorm:"-" json:"-"`
	Prefixes string `gorm:"column:prefixes;type:varchar(100)" json:"Prefixes"`
}

type CloudFileList struct {
	*gorm.DB    `gorm:"-" json:"-"`
	ID          int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"`
	UpdateTime  time.Time `gorm:"column:update_time;autoCreateTime" json:"update_time"`
	LableID     int64     `gorm:"column:lable_id;type:bigint(20)" json:"lable_id"`     // 标签id
	FileSize    int64     `gorm:"column:file_size;type:bigint(64)" json:"file_size"`   // 文件大小
	FileName    string    `gorm:"column:file_name;type:varchar(255)" json:"file_name"` // 文件名称
	State       int16     `gorm:"column:state;type:smallint(4)" json:"state"`          // 1上传成功,2,已导入filcoin网络,3.上链中9.上链
	ContentType string    `gorm:"column:content_type;type:varchar(255)" json:"content_type"`
	DelFlag     bool      `gorm:"column:del_flag;type:tinyint(1)" json:"del_flag"`
	Lable       string    `gorm:"column:lable;type:varchar(100)" json:"lanleName"`
}

type CloudFile struct {
	*gorm.DB     `gorm:"-" json:"-"`
	ID           int64     `gorm:"primaryKey;column:id;type:bigint(20);not null" json:"-"`
	CreateTime   time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	UpdateTime   time.Time `gorm:"column:update_time;autoCreateTime" json:"update_time"`
	PlatformID   int64     `gorm:"column:platform_id;type:bigint(20)" json:"platform_id"`      // 平台id
	SubjectID    int64     `gorm:"column:subject_id;type:bigint(20)" json:"subject_id"`        // 数据主体ID
	LableID      int64     `gorm:"column:lable_id;type:bigint(20)" json:"lable_id"`            // 标签id
	FileSize     int64     `gorm:"column:file_size;type:bigint(64)" json:"file_size"`          // 文件大小
	FileName     string    `gorm:"column:file_name;type:varchar(255)" json:"file_name"`        // 文件名称
	FileHash     string    `gorm:"column:file_hash;type:varchar(255)" json:"file_hash"`        // 文件hash
	UserPath     string    `gorm:"column:user_path;type:varchar(500)" json:"user_path"`        // 用户的文件目录
	State        int16     `gorm:"column:state;type:smallint(4)" json:"state"`                 // 1上传成功,2,已导入filcoin网络,3.上链中9.上链
	UploadSource int16     `gorm:"column:upload_source;type:smallint(1)" json:"upload_source"` // 上传来源 1-管理后台 2-存储系统接口 3-本地导入
	ContentType  string    `gorm:"column:content_type;type:varchar(255)" json:"content_type"`
	DelFlag      bool      `gorm:"column:del_flag;type:tinyint(1)" json:"del_flag"`
	DistState    int16     `gorm:"column:dist_state;type:smallint(4)" json:"dist_state"` //1-未分配 9-已分配
	Encryption   bool      `gorm:"column:encryption;type:tinyint(1)" json:"encryption"`  //0-明文，1-加密文件

}

//GetFileType 1.热存储	2.冷热存储	3.冷存储
func (u *CloudFileList) GetFileType() int16 {
	if u.DelFlag {
		return 3
	} else {
		if u.State == 9 {
			return 2
		} else {
			return 1
		}
	}
}

//GetFileType 1.热存储	2.冷热存储	3.冷存储
func (u *CloudFile) GetFileType() int16 {
	if u.DelFlag {
		return 3
	} else {
		if u.State == 9 {
			return 2
		} else {
			return 1
		}
	}
}

//获取文件夹列表
func (u *CloudFileList) GetPrefixes(folder string, encryption int) ([]CloudFilePrefixes, error) {
	var cloudFile []CloudFilePrefixes

	encryptionStr := ""
	if encryption == 0 {
		encryptionStr = " AND cloud_file.encryption = false "
	} else if encryption == 1 {
		encryptionStr = " AND cloud_file.encryption = true "
	}

	err := u.Table(u.TableName()).Select(" DISTINCT concat(substring_index(substr(user_path,length( ? )+1),'/',1),'/') as prefixes ", folder).Where("(del_flag = false OR (del_flag = true AND state = 9)) AND user_path LIKE ?  "+encryptionStr,
		folder+"%/%").Find(&cloudFile).Error
	if err != nil {
		return nil, err
	} else {
		return cloudFile, nil
	}
}

//GetFileList 根据平台id获取和 文件夹名称获取文件夹下的文件列表
func (u *CloudFileList) GetFileList(folder string, encryption, page, pageSize int) ([]CloudFileList,int64, error) {
	var cloudFile []CloudFileList

	encryptionStr := ""
	if encryption == 0 {
		encryptionStr = " AND cloud_file.encryption = false "
	} else if encryption == 1 {
		encryptionStr = " AND cloud_file.encryption = true "
	}

	var count int64
	if err:=u.Table(u.TableName()).Joins(" left join cloud_files_lables cfl on lable_id=cfl.id and cfl.del_flag = false WHERE (cloud_file.del_flag = false OR (cloud_file.del_flag = true AND cloud_file.state = 9)) AND cloud_file.user_path LIKE ? AND cloud_file.user_path not LIKE ? "+encryptionStr,
		folder+"%", folder+"%/%").Count(&count).Error;err!=nil {
		fmt.Println("报错：", err)
		return nil, 0, err
	}
	if count==0 {
		return cloudFile, 0, nil
	}

	if page == 0 || pageSize == 0 {
		err := u.Table(u.TableName()).Select("cloud_file.id,cloud_file.update_time,cloud_file.lable_id,cloud_file.file_size,cloud_file.file_name,cloud_file.state,cloud_file.content_type,cloud_file.del_flag,lable").Joins(" left join cloud_files_lables cfl on lable_id=cfl.id and cfl.del_flag = false WHERE (cloud_file.del_flag = false OR (cloud_file.del_flag = true AND cloud_file.state = 9)) AND cloud_file.user_path LIKE ? AND cloud_file.user_path not LIKE ? "+encryptionStr,
			folder+"%", folder+"%/%").Order(" id desc").Limit(5000).Offset(0).Find(&cloudFile).Error
		if err != nil {
			return nil,0, err
		} else {
			return cloudFile, count, nil
		}
	} else {
		err := u.Table(u.TableName()).Select("cloud_file.id,cloud_file.update_time,cloud_file.lable_id,cloud_file.file_size,cloud_file.file_name,cloud_file.state,cloud_file.content_type,cloud_file.del_flag,lable").Joins(" left join cloud_files_lables cfl on lable_id=cfl.id and cfl.del_flag = false WHERE (cloud_file.del_flag = false OR (cloud_file.del_flag = true AND cloud_file.state = 9)) AND cloud_file.user_path LIKE ? AND cloud_file.user_path not LIKE ? "+encryptionStr,
			folder+"%", folder+"%/%").Order(" id desc").Limit(pageSize).Offset((page - 1) * pageSize).Find(&cloudFile).Error
		if err != nil {
			return nil,0, err
		} else {
			return cloudFile, count, nil
		}
	}
}

//GetFileList 根据平台id获取和 文件夹名称获取文件夹下的文件列表
func (u *CloudFile) GetFileList(folder string, encryption int) ([]CloudFile, error) {
	var cloudFile []CloudFile

	encryptionStr := ""
	if encryption == 0 {
		encryptionStr = " AND encryption = false "
	} else if encryption == 1 {
		encryptionStr = " AND encryption = true "
	}

	err := u.Table(u.TableName()).Where("(del_flag = false OR (del_flag = true AND state = 9)) AND user_path LIKE ? AND user_path not LIKE ? "+encryptionStr,
		folder+"%", folder+"%/%").Find(&cloudFile).Error
	if err != nil {
		return nil, err
	} else {
		return cloudFile, nil
	}
}

//PutFile 增加一个文件
func (u *CloudFile) PutFile(platformId int64, subjectId int64, bucket string, name string, size int64, eTag string, contentType string, encryption bool, lable int64) error {

	index := strings.LastIndex(name, "/") + 1

	fileName := name[index:]
	cloudFile := CloudFile{
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
		PlatformID:  platformId,
		SubjectID:   subjectId,
		FileSize:    size,
		FileName:    fileName,
		UserPath:    bucket + "/" + name,
		FileHash:    eTag,
		ContentType: contentType,
		State:       0,
		DelFlag:     false,
		DistState:   1,
		Encryption:  encryption,
		LableID:     lable,
	}
	err := u.Table(u.TableName()).Where("(del_flag = false OR (del_flag = true AND state = 9)) AND user_path = ?", bucket+"/"+name).FirstOrCreate(&cloudFile).Error

	if err != nil {
		return err
	} else {
		return nil
	}
}

//UpdateFile 修改一个文件状态为删除状态 只有状态为0：上传成功  9：上链成功 才能进行删除操作
func (u *CloudFile) DeleteFile(bucket string, name string) error {
	fmt.Print("bucket", bucket)
	if strings.HasSuffix(bucket, "/") {
		bucket = strings.TrimRight(bucket, "/")
	}
	fmt.Print("bucket", bucket)

	err := u.Table(u.TableName()).Where("(state = ? OR state = ?) AND user_path = ?", 0, 9, bucket+"/"+name).Update("del_flag", true, "update_time", time.Now()).Error

	if err != nil {
		return err
	} else {
		return nil
	}
}

//CanDeleteFile 只有状态为 0 或者 9 的文件才能被删除
func (u *CloudFile) CanDeleteFile(bucket string, name string) bool {
	var cloudFile CloudFile
	err := u.Table(u.TableName()).Where("user_path = ? ",
		bucket+"/"+name).Find(&cloudFile).Error

	if err != nil || (cloudFile.State == 0 || cloudFile.State == 9) {
		return true
	} else {
		return false
	}

}

//CanPutFile 插入文件的时候先判断是否已经存在（没有被删 或者 已经上链）
func (u *CloudFile) CanPutFile(bucket string, name string) bool {
	var cloudFile CloudFile
	result := u.Table(u.TableName()).Where("(del_flag = false OR (del_flag = true AND state = 9)) AND user_path = ?", bucket+"/"+name).Find(&cloudFile)
	err := result.Error
	if err != nil {
		return false
	}
	if n := result.RowsAffected; n > 0 {
		return true
	}
	return false
}

package aliyun

import (
    "io"
    "io/ioutil"
    "log"
    "errors"
    "strings"

    "github.com/aliyun/aliyun-oss-go-sdk/oss"

    "github.com/deatil/go-filesystem/filesystem/adapter"
    "github.com/deatil/go-filesystem/filesystem/interfaces"
)

type ALiYunOSS struct {
    // 默认适配器基类
    adapter.Adapter

    AccessKeyId     string
    AccessKeySecret string
    Endpoint        string
    BucketName      string
}

// 初始化配置
func (this *ALiYunOSS) WithConfig(
    accessKeyId string,
    accessKeySecret string,
    endpoint string,
    bucketName string,
) *ALiYunOSS {
    this.AccessKeyId = accessKeyId
    this.AccessKeySecret = accessKeySecret
    this.Endpoint = endpoint
    this.BucketName = bucketName

    return this
}

// 上传
func (this *ALiYunOSS) Write(path string, contents string, conf interfaces.Config) (map[string]interface{}, error) {
    bucket, err := this.getBucket()
    if err != nil {
        this.handleError(err)

        return nil, errors.New("function AliyunOSS.NewBucket() Failed, err:" + err.Error())
    }

    // 上传文件流
    err = bucket.PutObject(path, strings.NewReader(contents))
    if err != nil {
        this.handleError(err)

        return nil, errors.New("function formUploader.Put() Failed, err:" + err.Error())
    }

    result := map[string]interface{}{
        "type": "file",
        "path": path,
    }

    if visibility := conf.Get("visibility"); visibility != nil {
        result["visibility"] = visibility.(string)
        this.SetVisibility(location, visibility.(string))
    }

    return result, nil
}

// 上传 Stream 文件类型
func (this *ALiYunOSS) WriteStream(path string, stream io.Reader, conf interfaces.Config) (map[string]interface{}, error) {
    bucket, err := this.getBucket()
    if err != nil {
        this.handleError(err)

        return nil, errors.New("function AliyunOSS.NewBucket() Failed, err:" + err.Error())
    }

    // 上传文件流
    err = bucket.PutObject(path, stream)
    if err != nil {
        this.handleError(err)

        return nil, errors.New("function formUploader.Put() Failed, err:" + err.Error())
    }

    result := map[string]interface{}{
        "type": "file",
        "path": path,
    }

    if visibility := conf.Get("visibility"); visibility != nil {
        result["visibility"] = visibility.(string)
        this.SetVisibility(location, visibility.(string))
    }

    return result, nil
}

// 读取
func (this *Adapter) Read(path string) (map[string]interface{}, error) {
    bucket, err := this.getBucket()
    if err != nil {
        this.handleError(err)

        return nil, errors.New("function AliyunOSS.NewBucket() Failed, err:" + err.Error())
    }

    body, err2 := bucket.GetObject(path)
        this.handleError(err2)

        return nil, errors.New("function AliyunOSS.Bucket.GetObject() Failed, err:" + err2.Error())
    }

    data, err3 := ioutil.ReadAll(body)
    body.Close()
    if err3 != nil {
        this.handleError(err3)

        return nil, errors.New("function ioutil.ReadAll() Failed, err:" + err3.Error())
    }

    return map[string]interface{}{
        "type": "file",
        "path": path,
        "contents": string(data),
    }, nil
}

// 删除
func (this *Adapter) Delete(path string) error {
    bucket, err := this.getBucket()
    if err != nil {
        this.handleError(err)

        return errors.New("function AliyunOSS.NewBucket() Failed, err:" + err.Error())
    }

    // 删除单个文件。objectName 表示删除OSS文件时需要指定包含文件后缀在内的完整路径，例如 abc/ddd.jpg。
    // 如需删除文件夹，请将objectName设置为对应的文件夹名称。
    // 如果文件夹非空，则需要将文件夹下的所有object删除后才能删除该文件夹。
    err2 := bucket.DeleteObject(path)
    if err2 != nil {
        this.handleError(err2)

        return errors.New("function bucketManager.Delete() Filed, err:" + err.Error())
    }

    return nil
}

// 删除文件夹
func (this *Adapter) DeleteDir(dirname string) error {
    return this.Delete(dirname)
}

// 设置文件的权限
func (this *Adapter) SetVisibility(path string, visibility string) (map[string]string, error) {
    data := map[string]string{
        "path": path,
        "visibility": visibility,
    }

    return data, nil
}

// GetBucket
func (this *ALiYunOSS) getBucket() (*oss.Bucket, error) {
    // 创建OSSClient实例。
    client, err := oss.New(this.Endpoint, this.AccessKeyId, this.AccessKeySecret)
    if err != nil {
        return nil, err
    }

    // 获取存储空间。
    bucket, err := client.Bucket(this.BucketName)
    if err != nil {
        return nil, err
    }

    return bucket, nil
}

// 输出错误
func (this *ALiYunOSS) handleError(err error) {
    log.Println("[go-filesystem-aliyun] Error:", err)
}

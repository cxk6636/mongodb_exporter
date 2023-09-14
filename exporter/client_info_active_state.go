package exporter

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"strconv"
	"strings"
)

type clientInfoActiveStateCollector struct {
	ctx    context.Context
	client *mongo.Client
	logger *logrus.Logger
}

func (d *clientInfoActiveStateCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *clientInfoActiveStateCollector) Collect(ch chan<- prometheus.Metric) {
	result := bson.M{}
	if err := d.client.Database("admin").RunCommand(d.ctx, bson.D{{Key: "currentOp", Value: 1}}).Decode(&result); err != nil {
		d.logger.Errorf("get client list info for db error: %s", err)
		return
	}

	inprog, ok := result["inprog"]
	if !ok {
		d.logger.Errorf("inprog field not found")
		return
	}
	inprogArray, ok := inprog.(primitive.A)
	if !ok {
		d.logger.Errorf("inprog field is not a primitive.A type")
		return
	}
	for _, elem := range inprogArray {
		if op, ok := elem.(bson.M); ok {
			// 获取连接信息中的opid、active、ip字段
			opid := op["opid"].(int32)
			var value float64
			connHost, ok := op["client"].(string)
			if !ok {
				connHost = "unknown"
			}
			connIP := strings.Split(connHost, ":")[0]
			//活跃的值是1，不活跃的值的0
			active := op["active"].(bool)
			if active {
				value = 1
			} else {
				value = 0
			}
			// 定义mongodb_client_list_info指标
			d := prometheus.NewDesc("mongodb_client_active_state", "The client connection info of MongoDB.", []string{"opid", "ip"}, nil)
			// 发送指标数据到采集通道中
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, value, strconv.Itoa(int(opid)), connIP)
		}
	}
}

var _ prometheus.Collector = (*clientInfoActiveStateCollector)(nil)

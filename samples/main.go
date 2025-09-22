// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"
)

func main() {
	fmt.Println("🚀 Canal Go Client 启动中...")
	fmt.Println(strings.Repeat("=", 60))

	// 192.168.199.17 替换成你的canal server的地址
	// example 替换成-e canal.destinations=example 你自己定义的名字
	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "test", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		log.Printf("❌ 连接Canal服务器失败: %v", err)
		os.Exit(1)
	}
	fmt.Println("✅ 成功连接到Canal服务器 127.0.0.1:11111")

	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)

	err = connector.Subscribe(".*\\..*")
	if err != nil {
		log.Printf("❌ 订阅失败: %v", err)
		os.Exit(1)
	}
	fmt.Println("✅ 成功订阅表: .*\\..*")
	fmt.Println("📊 开始监听数据库变更...")
	fmt.Println(strings.Repeat("=", 60))

	processedCount := 0
	for {

		message, err := connector.Get(100, nil, nil)
		if err != nil {
			log.Printf("❌ 获取消息失败: %v", err)
			os.Exit(1)
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Printf("\r⏳ 等待数据变更... (已处理: %d 条记录)", processedCount)
			continue
		}

		fmt.Printf("\n📦 收到批次 #%d, 包含 %d 条记录\n", batchId, len(message.Entries))
		printEntry(message.Entries)
		processedCount += len(message.Entries)
		fmt.Println(strings.Repeat("-", 60))

	}
}

func printEntry(entrys []pbe.Entry) {
	for i := range entrys {
		entry := &entrys[i] // 使用指针避免复制锁
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)

		eventType := rowChange.GetEventType()
		header := entry.GetHeader()

		// 获取操作类型的emoji和颜色
		var eventEmoji, eventColor string
		switch eventType {
		case pbe.EventType_INSERT:
			eventEmoji = "➕"
			eventColor = "\033[32m" // 绿色
		case pbe.EventType_UPDATE:
			eventEmoji = "🔄"
			eventColor = "\033[33m" // 黄色
		case pbe.EventType_DELETE:
			eventEmoji = "❌"
			eventColor = "\033[31m" // 红色
		default:
			eventEmoji = "ℹ️"
			eventColor = "\033[36m" // 青色
		}

		fmt.Printf("\n%s%s %s 操作\033[0m\n", eventColor, eventEmoji, eventType)
		fmt.Printf("📄 数据库: %s.%s\n", header.GetSchemaName(), header.GetTableName())
		fmt.Printf("📍 Binlog: %s:%d\n", header.GetLogfileName(), header.GetLogfileOffset())
		fmt.Printf("⏰ 执行时间: %s\n", time.Unix(header.GetExecuteTime()/1000, 0).Format("2006-01-02 15:04:05"))

		rowCount := len(rowChange.GetRowDatas())
		fmt.Printf("📊 影响行数: %d\n", rowCount)

		for rowIndex, rowData := range rowChange.GetRowDatas() {
			if rowCount > 1 {
				fmt.Printf("\n  【第 %d 行数据】\n", rowIndex+1)
			}
			switch eventType {
			case pbe.EventType_DELETE:
				fmt.Println("  🗑️  删除前的数据:")
				// 使用简洁版显示，如果需要表格版可以换成 printColumn
				printColumnSimple(rowData.GetBeforeColumns(), "    ")
			case pbe.EventType_INSERT:
				fmt.Println("  ✨ 新增的数据:")
				printColumnSimple(rowData.GetAfterColumns(), "    ")
			default:
				fmt.Println("  📝 修改前的数据:")
				printColumnSimple(rowData.GetBeforeColumns(), "    ")
				fmt.Println("  📝 修改后的数据:")
				printColumnSimple(rowData.GetAfterColumns(), "    ")
			}
		}
	}
}

func printColumn(columns []*pbe.Column, indent string) {
	if len(columns) == 0 {
		fmt.Printf("%s(无数据)\n", indent)
		return
	}

	// 计算各列的最大宽度
	maxNameLen := 8   // 最小列名宽度
	maxValueLen := 15 // 最小值宽度

	for _, col := range columns {
		nameLen := len(col.GetName())
		if nameLen > maxNameLen {
			maxNameLen = nameLen
		}

		valueLen := len(col.GetValue())
		if valueLen > maxValueLen && valueLen <= 30 {
			maxValueLen = valueLen
		}
	}

	// 限制最大宽度
	if maxNameLen > 20 {
		maxNameLen = 20
	}
	if maxValueLen > 30 {
		maxValueLen = 30
	}

	// 使用简单的ASCII字符绘制表格
	fmt.Printf("%s+%s+%s+%s+%s+\n", indent,
		strings.Repeat("-", maxNameLen+2),
		strings.Repeat("-", maxValueLen+2),
		strings.Repeat("-", 8),
		strings.Repeat("-", 10))

	fmt.Printf("%s| %-*s | %-*s | %-6s | %-8s |\n", indent,
		maxNameLen, "列名",
		maxValueLen, "值",
		"类型", "是否更新")

	fmt.Printf("%s+%s+%s+%s+%s+\n", indent,
		strings.Repeat("-", maxNameLen+2),
		strings.Repeat("-", maxValueLen+2),
		strings.Repeat("-", 8),
		strings.Repeat("-", 10))

	// 打印数据行
	for _, col := range columns {
		name := col.GetName()
		if len(name) > maxNameLen {
			name = name[:maxNameLen-3] + "..."
		}

		value := col.GetValue()
		if len(value) > maxValueLen {
			value = value[:maxValueLen-3] + "..."
		}

		sqlType := col.GetSqlType()
		typeStr := getSqlTypeName(sqlType)

		updated := "否"
		if col.GetUpdated() {
			updated = "是"
		}

		fmt.Printf("%s| %-*s | %-*s | %-6s | %-8s |\n", indent,
			maxNameLen, name,
			maxValueLen, value,
			typeStr, updated)
	}

	// 打印表尾
	fmt.Printf("%s+%s+%s+%s+%s+\n", indent,
		strings.Repeat("-", maxNameLen+2),
		strings.Repeat("-", maxValueLen+2),
		strings.Repeat("-", 8),
		strings.Repeat("-", 10))
}

// getSqlTypeName 将Java SQL Types数字转换为可读的类型名称
func getSqlTypeName(sqlType int32) string {
	switch sqlType {
	case -7:
		return "BIT"
	case -6:
		return "TINYINT"
	case 5:
		return "SMALLINT"
	case 4:
		return "INTEGER"
	case -5:
		return "BIGINT"
	case 6:
		return "FLOAT"
	case 8:
		return "DOUBLE"
	case 3:
		return "DECIMAL"
	case 2:
		return "NUMERIC"
	case 1:
		return "CHAR"
	case 12:
		return "VARCHAR"
	case -1:
		return "LONGVARCHAR"
	case 91:
		return "DATE"
	case 92:
		return "TIME"
	case 93:
		return "TIMESTAMP"
	case -2:
		return "BINARY"
	case -3:
		return "VARBINARY"
	case -4:
		return "LONGVARBINARY"
	case 2004:
		return "BLOB"
	case 2005:
		return "CLOB"
	case 16:
		return "BOOLEAN"
	case 2009:
		return "SQLXML"
	case 2011:
		return "NCLOB"
	case -15:
		return "NCHAR"
	case -9:
		return "NVARCHAR"
	case -16:
		return "LONGNVARCHAR"
	case 0:
		return "NULL"
	default:
		return fmt.Sprintf("TYPE_%d", sqlType)
	}
}

// 简洁版的列数据显示，不使用表格
func printColumnSimple(columns []*pbe.Column, indent string) {
	if len(columns) == 0 {
		fmt.Printf("%s(无数据)\n", indent)
		return
	}

	for _, col := range columns {
		value := col.GetValue()
		if len(value) > 50 {
			value = value[:47] + "..."
		}

		updated := ""
		if col.GetUpdated() {
			updated = " [已更新]"
		}

		// 添加类型信息
		typeInfo := getSqlTypeName(col.GetSqlType())

		fmt.Printf("%s%s (%s): %s%s\n", indent, col.GetName(), typeInfo, value, updated)
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

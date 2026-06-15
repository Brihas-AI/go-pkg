package utils

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
)

type WaitGroup struct {
	sync.WaitGroup
}

func (wg *WaitGroup) Done() {
	if r := recover(); r != nil {
		log.WithFields(log.Fields{
			"panic":      r,
			"stacktrace": strings.Split(string(debug.Stack()), "\n"),
			"source":     "wg.Done",
		}).Error("goroutine panic")
	}
	wg.Add(-1)
}

func ToString(v interface{}) string {
	switch val := v.(type) {
	case primitive.ObjectID:
		return val.Hex()
	case fmt.Stringer:
		return val.String()
	case string:
		return val
	case int, int8, int16, int32, int64:
		return strconv.FormatInt(reflect.ValueOf(val).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflect.ValueOf(val).Uint(), 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case nil:
		return ""
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			return ""
		}
		return fmt.Sprintf("%v", v)
	}
}

func ToInt(value interface{}) int {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return int(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		return int(reflect.ValueOf(v).Uint())
	case float32:
		return int(v)
	case float64:
		return int(v)
	case string:
		i, err := strconv.Atoi(v)
		if err == nil {
			return i
		}
		return 0
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			return ToInt(rv.Elem().Interface())
		}
		return 0
	}
}

func ToFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float64, float32:
		return reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint())
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f
		}
		return 0
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			return ToFloat64(rv.Elem().Interface())
		}
		return 0
	}
}

func GetNthFibonacciNumber(n int) int {
	a, b := 1, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return a
}

func GenerateHexID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func HashStringToUint32(value string) uint32 {
	hash := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint32(hash[:4])
}

func GenerateMD5HashID(input string) string {
	hash := md5.Sum([]byte(input))
	return hex.EncodeToString(hash[:])
}

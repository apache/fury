package meta

// 首先定义几种编码格式, 枚举类型
type Encoding uint8

const (
	UTF_8                     Encoding = 0x00
	LOWER_SPECIAL             Encoding = 0x01
	LOWER_UPPER_DIGIT_SPECIAL Encoding = 0x02
	FIRST_TO_LOWER_SPECIAL    Encoding = 0x03
	ALL_TO_LOWER_SPECIAL      Encoding = 0x04
)

// 定义 MetaString 类, 用于存储对元信息序列化的结果
type MetaString struct {
	inputString  string   // 输入字符串
	encoding     Encoding // 编码方式
	specialChar1 byte
	specialChar2 byte
	outputBytes  []byte // 序列化结果
	numChars     int
	numBits      int
}

// 定义 Getter, Setter, to_string 方法

func (ms *MetaString) GetInputString() string { return ms.inputString }

func (ms *MetaString) GetEncoding() Encoding { return ms.encoding }

func (ms *MetaString) GetSpecialChar1() byte { return ms.specialChar1 }

func (ms *MetaString) GetSpecialChar2() byte { return ms.specialChar2 }

func (ms *MetaString) GetOutputBytes() []byte { return ms.outputBytes }

func (ms *MetaString) GetNumChars() int { return ms.numChars }

func (ms *MetaString) GetNumBits() int { return ms.numBits }

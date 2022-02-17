package market

/**
 * @Description: 大市场市场号
 * @return unc
 */
const (
	SH  = 0x10
	SZ  = 0x20
	RF  = 0x30
	CF  = 0x40
	FI  = 0x58
	SW  = 0x68
	IF  = 0x80
	IB  = 0x88
	ST  = 0x90
	SZK = 0x98
	EU  = 0xA0
	NY  = 0xA8
	NQ  = 0xB8
	HK  = 0xB0
)

/*
 * @Description: 大市场对应的后缀
 */
var marketSuffix = map[int]string{
	SH:  "sh",
	SZ:  "sz",
	RF:  "rf",
	CF:  "cf",
	FI:  "fi",
	SW:  "sw",
	IF:  "if",
	IB:  "ib",
	ST:  "st",
	SZK: "szk",
	EU:  "eu",
	NY:  "ny",
	NQ:  "nq",
	HK:  "hk",
}

/*GetSchema
 * @Description: 通过市场号获取所属市场（对应mysql中的数据库名
 * @param market
 * @return schema
 */
func GetSchema(market int) (schema string) {
	Mar := getMARKET(market)
	suffix, ok := marketSuffix[Mar]
	if !ok {
		suffix = ""
	}
	return suffix
}

/**
 * @Description: 获取大市场
 * @param market
 * @return int
 */
func getMARKET(market int) int {
	return market & 0xF8
}

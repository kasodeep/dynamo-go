package codec

import (
	"encoding/json"

	"github.com/kasodeep/dynamo-go/member"
)

func EncodeMembers(members []member.Member) []byte {
	b, _ := json.Marshal(members)
	return b
}

func DecodeMembers(data []byte) []member.Member {
	var members []member.Member
	_ = json.Unmarshal(data, &members)
	return members
}

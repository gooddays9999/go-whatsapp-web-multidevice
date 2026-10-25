package send

type MessageRequest struct {
	BaseRequest
	MessageID      string   `json:"message_id,omitempty" form:"message_id"`
	Message        string   `json:"message" form:"message"`
	ReplyMessageID *string  `json:"reply_message_id" form:"reply_message_id"`
	Mentions       []string `json:"mentions,omitempty" form:"mentions"` // List of phone numbers/JIDs to mention (ghost mentions)
}

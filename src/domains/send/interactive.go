package send

// InteractiveRequest sends a WhatsApp interactive message (native_flow buttons
// such as cta_url), optionally with an image header.
//
// ProtoJSON is the WhatsApp InteractiveMessage encoded as protobuf-JSON — the
// same shape WhatsApp itself uses, e.g. {"body":{"text":"..."},
// "nativeFlowMessage":{"buttons":[{"name":"cta_url","buttonParamsJSON":"..."}]}}.
// ImageURL, when set, is downloaded and attached as the header image.
type InteractiveRequest struct {
	BaseRequest
	ProtoJSON      string  `json:"proto_json" form:"proto_json"`
	ImageURL       string  `json:"image_url,omitempty" form:"image_url"`
	ReplyMessageID *string `json:"reply_message_id,omitempty" form:"reply_message_id"`
}

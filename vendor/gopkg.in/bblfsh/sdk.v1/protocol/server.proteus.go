package protocol

import (
	"golang.org/x/net/context"
)

type protocolServiceServer struct {
}

func NewProtocolServiceServer() *protocolServiceServer {
	return &protocolServiceServer{}
}
func (s *protocolServiceServer) NativeParse(ctx context.Context, in *NativeParseRequest) (result *NativeParseResponse, err error) {
	result = new(NativeParseResponse)
	result = NativeParse(in)
	return
}
func (s *protocolServiceServer) Parse(ctx context.Context, in *ParseRequest) (result *ParseResponse, err error) {
	result = new(ParseResponse)
	result = Parse(in)
	return
}
func (s *protocolServiceServer) SupportedLanguages(ctx context.Context, in *SupportedLanguagesRequest) (result *SupportedLanguagesResponse, err error) {
	result = new(SupportedLanguagesResponse)
	result = SupportedLanguages(in)
	return
}
func (s *protocolServiceServer) Version(ctx context.Context, in *VersionRequest) (result *VersionResponse, err error) {
	result = new(VersionResponse)
	result = Version(in)
	return
}

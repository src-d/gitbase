package protocol

// Parse uses DefaultService to process the given parsing request to get the UAST.
//proteus:generate
func Parse(req *ParseRequest) *ParseResponse {
	if r := checkDefaultService(); r != nil {
		return &ParseResponse{Response: *r}
	}

	return DefaultService.Parse(req)
}

// NativeParse uses DefaultService to process the given parsing request to get
// the AST.
//proteus:generate
func NativeParse(req *NativeParseRequest) *NativeParseResponse {
	if r := checkDefaultService(); r != nil {
		return &NativeParseResponse{Response: *r}
	}

	return DefaultService.NativeParse(req)
}

// Version uses DefaultVersioner to process the given version request to get the version.
//proteus:generate
func Version(req *VersionRequest) *VersionResponse {
	if r := checkDefaultService(); r != nil {
		return &VersionResponse{Response: *r}
	}

	return DefaultService.Version(req)
}

// SupportedLanguages uses DefaultService to process the given SupportedLanguagesRequest to get the supported drivers.
//proteus:generate
func SupportedLanguages(req *SupportedLanguagesRequest) *SupportedLanguagesResponse {
	if r := checkDefaultService(); r != nil {
		return &SupportedLanguagesResponse{Response: *r}
	}

	return DefaultService.SupportedLanguages(req)
}

func checkDefaultService() *Response {
	if DefaultService == nil {
		return &Response{
			Status: Fatal,
			Errors: []string{"no default service registered"},
		}
	}

	return nil
}

package normalizer

import (
	. "gopkg.in/bblfsh/sdk.v2/uast/transformer"
)

var Preprocess = Transformers([][]Transformer{
	{
		// ResponseMetadata is a transform that trims response metadata from AST.
		//
		// https://godoc.org/gopkg.in/bblfsh/sdk.v2/uast#ResponseMetadata
		ResponseMetadata{
			TopLevelIsRootNode: false,
		},
	},
	{Mappings(Preprocessors...)},
}...)

var Normalize = Transformers([][]Transformer{

	{Mappings(Normalizers...)},
}...)

// Preprocessors is a block of AST preprocessing rules rules.
var Preprocessors = []Mapping{
	// ObjectToNode defines how to normalize common fields of native AST
	// (like node type, token, positional information).
	//
	// https://godoc.org/gopkg.in/bblfsh/sdk.v2/uast#ObjectToNode
	ObjectToNode{
		InternalTypeKey: "...", // native AST type key name
	}.Mapping(),
}

// Normalizers is the main block of normalization rules to convert native AST to semantic UAST.
var Normalizers = []Mapping{}

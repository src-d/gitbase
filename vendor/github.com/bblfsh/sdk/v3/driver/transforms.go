package driver

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/transformer"
)

// Transforms describes a set of AST transformations the driver requires.
//
// The pipeline can be illustrated as:
//         ( AST )--------------> ( ModeNative )
//            V
//      [ Preprocess ]
//    [ PreprocessCode ]--------> ( ModePreprocessed )
//            |
//            |----------------\
//            |           [ Normalize ]
//     [ Annotations ] <-------/
//            |
//            V
//        [ Code ]-------------\
//            |           [ Namespace ]
//            |                |
//            V                V
//     ( ModeAnnotated ) ( ModeSemantic )
type Transforms struct {
	// Namespace for native AST nodes of this language. Only enabled in Semantic mode.
	//
	// Namespace will be set at the end of the pipeline, thus all transforms can
	// use type names without the driver namespace.
	Namespace string

	// Preprocess stage normalizes native AST for both Annotated and Semantic stages.
	//
	// It usually:
	//  * changes type key to uast.KeyType
	//  * restructures positional information
	//  * fixes any issues with native AST structure
	Preprocess []transformer.Transformer

	// PreprocessCode stage runs code-assisted transformations after the Preprocess stage.
	// It can be used to fix node tokens or positional information based on the source.
	PreprocessCode []transformer.CodeTransformer

	// Normalize stage converts a known native AST structures to a canonicalized
	// high-level AST representation (UAST). It is executed after PreprocessCode
	// and before the Annotations stage.
	Normalize []transformer.Transformer

	// Annotations stage applies UAST role annotations and is executed after
	// Semantic stage, or after PreprocessCode if Semantic is disabled.
	//
	// It also changes token key to uast.KeyToken. It should not be done in the
	// Preprocess stage, because Semantic annotations are easier on clean native AST.
	Annotations []transformer.Transformer
}

// Do applies AST transformation pipeline for specified AST subtree.
//
// Mode can be specified to stop the pipeline at a specific abstraction level.
func (t Transforms) Do(rctx context.Context, mode Mode, code string, nd nodes.Node) (nodes.Node, error) {
	sp, ctx := opentracing.StartSpanFromContext(rctx, "uast.Transform")
	defer sp.Finish()

	if mode > ModeSemantic {
		return nil, ErrModeNotSupported.New()
	}
	if mode == 0 {
		mode = ModeDefault
	}
	if mode == ModeNative {
		return nd, nil
	}

	return t.do(ctx, mode, code, nd)
}

func (t Transforms) do(ctx context.Context, mode Mode, code string, nd nodes.Node) (nodes.Node, error) {
	var err error
	runAll := func(name string, list []transformer.Transformer) error {
		sp, _ := opentracing.StartSpanFromContext(ctx, "uast.Transform."+name)
		defer sp.Finish()

		for _, t := range list {
			nd, err = t.Do(nd)
			if err != nil {
				return err
			}
		}
		return nil
	}
	runAllCode := func(name string, list []transformer.CodeTransformer) error {
		sp, _ := opentracing.StartSpanFromContext(ctx, "uast.Transform."+name)
		defer sp.Finish()

		for _, ct := range list {
			t := ct.OnCode(code)
			nd, err = t.Do(nd)
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Preprocess AST and optionally use the second pre-processing stage
	// that can access the source code (to fix tokens, for example).
	if err := runAll("preprocess", t.Preprocess); err != nil {
		return nd, err
	}
	if err := runAllCode("preprocess-code", t.PreprocessCode); err != nil {
		return nd, err
	}

	// First run Semantic mode (UAST canonicalization).
	// It's considered a more high-level representation, but it needs
	// a clean AST to run, so we execute it before Annotated mode.
	if mode >= ModeSemantic {
		if err := runAll("semantic", t.Normalize); err != nil {
			return nd, err
		}
	}

	// Next run Annotated mode. It won't see nodes converted by Semantic nodes,
	// because it expects a clean native AST.
	// This is intentional — Semantic nodes are already defined with specific
	// roles in mind, thus they shouldn't be annotated further on this stage.
	if mode >= ModeAnnotated {
		if err := runAll("annotated", t.Annotations); err != nil {
			return nd, err
		}
	}

	// All native nodes should have a namespace in Semantic mode.
	// Set if it was specified in the transform configuration.
	if mode >= ModeSemantic && t.Namespace != "" {
		tr := transformer.DefaultNamespace(t.Namespace)
		if err := runAll("namespace", []transformer.Transformer{tr}); err != nil {
			return nd, err
		}
	}

	return nd, nil
}
